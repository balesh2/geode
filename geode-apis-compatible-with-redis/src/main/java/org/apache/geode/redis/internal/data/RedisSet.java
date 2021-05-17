/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package org.apache.geode.redis.internal.data;

import static java.util.Collections.emptyList;
import static org.apache.geode.redis.internal.data.RedisDataType.REDIS_SET;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.regex.Pattern;

import it.unimi.dsi.fastutil.bytes.ByteArrays;
import it.unimi.dsi.fastutil.objects.ObjectOpenCustomHashSet;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import org.apache.geode.DataSerializer;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.redis.internal.delta.AddsDeltaInfo;
import org.apache.geode.redis.internal.delta.DeltaInfo;
import org.apache.geode.redis.internal.delta.RemsDeltaInfo;
import org.apache.geode.redis.internal.netty.Coder;

public class RedisSet extends AbstractRedisData {
  private ObjectOpenCustomHashSet<byte[]> members;

  // the following constants were calculated using reflection and math. you can find the tests for
  // these values in RedisSetTest, which show the way these numbers were calculated. the constants
  // have the advantage of saving us a lot of computation that would happen every time a new key was
  // added. if our internal implementation changes, these values may be incorrect. the tests will
  // catch this change. an increase in overhead should be carefully considered.
  // Note: the per member overhead is known to not be constant. it changes as more members are
  // added, and/or as the members get longer
  protected static final int BASE_REDIS_SET_OVERHEAD = 136;
  protected static final int PER_MEMBER_OVERHEAD = 42;

  private int sizeInBytes = BASE_REDIS_SET_OVERHEAD;
  // The number of elements that the backing set can hold before needing to resize
  public int backingSetCapacity = 1;

  RedisSet(Collection<byte[]> members) {
    this.members = new ObjectOpenCustomHashSet<>(members.size(), ByteArrays.HASH_STRATEGY);
    for (byte[] member : members) {
      membersAdd(member);
    }
  }

  /**
   * For deserialization only.
   */
  public RedisSet() {}

  Pair<BigInteger, List<Object>> sscan(Pattern matchPattern, int count, BigInteger cursor) {
    List<Object> returnList = new ArrayList<>();
    int size = members.size();
    BigInteger beforeCursor = new BigInteger("0");
    int numElements = 0;
    int i = -1;
    for (byte[] value : members) {
      i++;
      if (beforeCursor.compareTo(cursor) < 0) {
        beforeCursor = beforeCursor.add(new BigInteger("1"));
        continue;
      }

      if (matchPattern != null) {
        if (matchPattern.matcher(Coder.bytesToString(value)).matches()) {
          returnList.add(value);
          numElements++;
        }
      } else {
        returnList.add(value);
        numElements++;
      }

      if (numElements == count) {
        break;
      }
    }

    Pair<BigInteger, List<Object>> scanResult;
    if (i >= size - 1) {
      scanResult = new ImmutablePair<>(new BigInteger("0"), returnList);
    } else {
      scanResult = new ImmutablePair<>(new BigInteger(String.valueOf(i + 1)), returnList);
    }
    return scanResult;
  }

  Collection<byte[]> spop(Region<RedisKey, RedisData> region, RedisKey key, int popCount) {
    int originalSize = scard();
    if (originalSize == 0) {
      return emptyList();
    }

    if (popCount >= originalSize) {
      region.remove(key, this);
      return this.members;
    }

    List<byte[]> popped = new ArrayList<>();
    byte[][] setMembers = members.toArray(new byte[originalSize][]);
    Random rand = new Random();
    while (popped.size() < popCount) {
      int idx = rand.nextInt(originalSize);
      byte[] memberToPop = setMembers[idx];
      if (memberToPop != null) {
        setMembers[idx] = null;
        popped.add(memberToPop);
        membersRemove(memberToPop);
      }
    }
    if (!popped.isEmpty()) {
      storeChanges(region, key, new RemsDeltaInfo(popped));
    }
    return popped;
  }

  Collection<byte[]> srandmember(int count) {
    int membersSize = members.size();
    boolean duplicatesAllowed = count < 0;
    if (duplicatesAllowed) {
      count = -count;
    }

    if (!duplicatesAllowed && membersSize <= count && count != 1) {
      return new ArrayList<>(members);
    }

    Random rand = new Random();

    byte[][] entries = members.toArray(new byte[membersSize][]);

    if (count == 1) {
      byte[] randEntry = entries[rand.nextInt(entries.length)];
      // Note using ArrayList because Collections.singleton has serialization issues.
      List<byte[]> result = new ArrayList<>(1);
      result.add(randEntry);
      return result;
    }
    if (duplicatesAllowed) {
      List<byte[]> result = new ArrayList<>(count);
      while (count > 0) {
        result.add(entries[rand.nextInt(entries.length)]);
        count--;
      }
      return result;
    } else {
      Set<byte[]> result = new HashSet<>();
      // Note that rand.nextInt can return duplicates when "count" is high
      // so we need to use a Set to collect the results.
      while (result.size() < count) {
        byte[] s = entries[rand.nextInt(entries.length)];
        result.add(s);
      }
      return result;
    }
  }

  public boolean sismember(byte[] member) {
    return members.contains(member);
  }

  public int scard() {
    return members.size();
  }

  @Override
  protected void applyDelta(DeltaInfo deltaInfo) {
    if (deltaInfo instanceof AddsDeltaInfo) {
      for (byte[] deltaMember : ((AddsDeltaInfo) deltaInfo).getAdds()) {
        membersAdd(deltaMember);
      }
    } else {
      for (byte[] deltaMember : ((RemsDeltaInfo) deltaInfo).getRemoves()) {
        membersRemove(deltaMember);
      }
    }
  }

  /**
   * Since GII (getInitialImage) can come in and call toData while other threads
   * are modifying this object, the striped executor will not protect toData.
   * So any methods that modify "members" needs to be thread safe with toData.
   */

  @Override
  public synchronized void toData(DataOutput out, SerializationContext context) throws IOException {
    super.toData(out, context);
    DataSerializer.writePrimitiveInt(members.size(), out);
    for (byte[] member : members) {
      DataSerializer.writeByteArray(member, out);
    }
    DataSerializer.writeInteger(sizeInBytes, out);
  }

  @Override
  public void fromData(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    int size = DataSerializer.readPrimitiveInt(in);
    members = new ObjectOpenCustomHashSet<>(size, ByteArrays.HASH_STRATEGY);
    for (int i = 0; i < size; ++i) {
      members.add(DataSerializer.readByteArray(in));
    }
    sizeInBytes = DataSerializer.readInteger(in);
  }

  @Override
  public int getDSFID() {
    return REDIS_SET_ID;
  }

  synchronized boolean membersAdd(byte[] memberToAdd) {
    boolean isAdded = members.add(memberToAdd);
    if (isAdded) {
      int length = memberToAdd.length;
      int memberOverhead = (length == 0) ? 0 : 8 * (3 + (length - 1) / 8);
      int resizeOverhead = calculateSetResizeOverhead(true);
      sizeInBytes += memberOverhead + resizeOverhead;
    }
    return isAdded;
  }

  boolean membersRemove(byte[] memberToRemove) {
    boolean isRemoved = members.remove(memberToRemove);
    if (isRemoved) {
      int length = memberToRemove.length;
      int memberOverhead = length == 0 ? 0 : 8 * (3 + (length - 1) / 8);
      int resizeOverhead = calculateSetResizeOverhead(false);
      sizeInBytes -= memberOverhead + resizeOverhead;
    }
    return isRemoved;
  }

  /**
   * @param membersToAdd members to add to this set; NOTE this list may by
   *        modified by this call
   * @param region the region this instance is stored in
   * @param key the name of the set to add to
   * @return the number of members actually added
   */
  long sadd(List<byte[]> membersToAdd, Region<RedisKey, RedisData> region, RedisKey key) {
    membersToAdd.removeIf(memberToAdd -> !membersAdd(memberToAdd));
    int membersAdded = membersToAdd.size();
    if (membersAdded != 0) {
      storeChanges(region, key, new AddsDeltaInfo(membersToAdd));
    }
    return membersAdded;
  }

  /**
   * @param membersToRemove members to remove from this set; NOTE this list may by
   *        modified by this call
   * @param region the region this instance is stored in
   * @param key the name of the set to remove from
   * @return the number of members actually removed
   */
  long srem(List<byte[]> membersToRemove, Region<RedisKey, RedisData> region, RedisKey key) {
    membersToRemove.removeIf(memberToRemove -> !membersRemove(memberToRemove));
    int membersRemoved = membersToRemove.size();
    if (membersRemoved != 0) {
      storeChanges(region, key, new RemsDeltaInfo(membersToRemove));
    }
    return membersRemoved;
  }

  /**
   * The returned set is a copy and will not be changed
   * by future changes to this instance.
   *
   * @return a set containing all the members in this set
   */
  @VisibleForTesting
  Set<byte[]> smembers() {
    return new HashSet<>(members);
  }

  @Override
  public RedisDataType getType() {
    return REDIS_SET;
  }

  @Override
  protected boolean removeFromRegion() {
    return members.isEmpty();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RedisSet)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    RedisSet redisSet = (RedisSet) o;

    if (redisSet.members.size() != members.size()) {
      return false;
    }
    for (byte[] member : members) {
      if (!redisSet.members.contains(member)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), members);
  }

  @Override
  public String toString() {
    return "RedisSet{" + super.toString() + ", " + "members=" + members + '}';
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }

  @Override
  public int getSizeInBytes() {
    return sizeInBytes;
  }

  // Calculates the number of times a ObjectOpenCustomHashSet has been resized
  int calculateSetResizeOverhead(boolean isAdd) {
    int overhead = 0;
    if (isAdd) {
      // Need to resize up
      if (backingSetCapacity < members.size()) {
        if (members.size() <= 3 && backingSetCapacity == 1) {
          backingSetCapacity = 3;
        } else {
          backingSetCapacity *= 2;
        }
        overhead = 1 << (2 + logBaseTwo(backingSetCapacity));
      }
    } else {
      // Need to resize down. We never resize back down to below a capacity of 12
      if (backingSetCapacity > 12 && backingSetCapacity / 4 == members.size() + 1) {
        overhead = 1 << (2 + logBaseTwo(backingSetCapacity));
        backingSetCapacity /= 2;
      }
    }
    return overhead;
  }

  static int logBaseTwo(int x) {
    if (x < 1) {
      throw new IllegalArgumentException("Cannot take the logarithm of numbers less than 1");
    }
    if (x < 2) {
      return 0;
    }
    int result = 0;
    while ((x >>= 1) >= 1) {
      result++;
    }
    return result;
  }
}
