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

import static org.apache.geode.redis.internal.data.NullRedisDataStructures.NULL_REDIS_SET;
import static org.apache.geode.redis.internal.data.RedisSet.BASE_REDIS_SET_OVERHEAD;
import static org.apache.geode.redis.internal.data.RedisSet.PER_MEMBER_OVERHEAD;
import static org.apache.geode.redis.internal.data.RedisSet.logBaseTwo;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.serialization.ByteArrayDataInput;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.size.ReflectionObjectSizer;

public class RedisSetTest {
  private final ReflectionObjectSizer reflectionObjectSizer = ReflectionObjectSizer.getInstance();

  @BeforeClass
  public static void beforeClass() {
    InternalDataSerializer.getDSFIDSerializer().registerDSFID(
        DataSerializableFixedID.REDIS_SET_ID,
        RedisSet.class);
  }

  @Test
  public void confirmSerializationIsStable() throws IOException, ClassNotFoundException {
    RedisSet set1 = createRedisSet(1, 2);
    int expirationTimestamp = 1000;
    set1.setExpirationTimestampNoDelta(expirationTimestamp);
    HeapDataOutputStream out = new HeapDataOutputStream(100);
    DataSerializer.writeObject(set1, out);
    ByteArrayDataInput in = new ByteArrayDataInput(out.toByteArray());
    RedisSet set2 = DataSerializer.readObject(in);
    assertThat(set2).isEqualTo(set1);
    assertThat(set2.getExpirationTimestamp())
        .isEqualTo(set1.getExpirationTimestamp())
        .isEqualTo(expirationTimestamp);
  }

  @Test
  public void confirmToDataIsSynchronized() throws NoSuchMethodException {
    assertThat(Modifier
        .isSynchronized(RedisSet.class
            .getMethod("toData", DataOutput.class, SerializationContext.class).getModifiers()))
                .isTrue();
  }

  private RedisSet createRedisSet(int m1, int m2) {
    return new RedisSet(Arrays.asList(new byte[] {(byte) m1}, new byte[] {(byte) m2}));
  }

  @Test
  public void equals_returnsFalse_givenDifferentExpirationTimes() {
    RedisSet set1 = createRedisSet(1, 2);
    set1.setExpirationTimestampNoDelta(1000);
    RedisSet set2 = createRedisSet(1, 2);
    set2.setExpirationTimestampNoDelta(999);
    assertThat(set1).isNotEqualTo(set2);
  }

  @Test
  public void equals_returnsFalse_givenDifferentValueBytes() {
    RedisSet set1 = createRedisSet(1, 2);
    set1.setExpirationTimestampNoDelta(1000);
    RedisSet set2 = createRedisSet(1, 3);
    set2.setExpirationTimestampNoDelta(1000);
    assertThat(set1).isNotEqualTo(set2);
  }

  @Test
  public void equals_returnsTrue_givenEqualValueBytesAndExpiration() {
    RedisSet set1 = createRedisSet(1, 2);
    int expirationTimestamp = 1000;
    set1.setExpirationTimestampNoDelta(expirationTimestamp);
    RedisSet set2 = createRedisSet(1, 2);
    set2.setExpirationTimestampNoDelta(expirationTimestamp);
    assertThat(set1).isEqualTo(set2);
    assertThat(set2.getExpirationTimestamp())
        .isEqualTo(set1.getExpirationTimestamp())
        .isEqualTo(expirationTimestamp);
  }

  @Test
  public void equals_returnsTrue_givenDifferentEmptySets() {
    RedisSet set1 = new RedisSet(Collections.emptyList());
    RedisSet set2 = NULL_REDIS_SET;
    assertThat(set1).isEqualTo(set2);
    assertThat(set2).isEqualTo(set1);
  }

  @Test
  public void sadd_stores_delta_that_is_stable() throws IOException {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    RedisSet set1 = createRedisSet(1, 2);
    byte[] member3 = new byte[] {3};
    ArrayList<byte[]> adds = new ArrayList<>();
    adds.add(member3);
    set1.sadd(adds, region, null);
    assertThat(set1.hasDelta()).isTrue();
    HeapDataOutputStream out = new HeapDataOutputStream(100);
    set1.toDelta(out);
    assertThat(set1.hasDelta()).isFalse();
    ByteArrayDataInput in = new ByteArrayDataInput(out.toByteArray());
    RedisSet set2 = createRedisSet(1, 2);
    assertThat(set2).isNotEqualTo(set1);
    set2.fromDelta(in);
    assertThat(set2).isEqualTo(set1);
  }

  @Test
  public void srem_stores_delta_that_is_stable() throws IOException {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    RedisSet set1 = createRedisSet(1, 2);
    byte[] member1 = new byte[] {1};
    ArrayList<byte[]> removes = new ArrayList<>();
    removes.add(member1);
    set1.srem(removes, region, null);
    assertThat(set1.hasDelta()).isTrue();
    HeapDataOutputStream out = new HeapDataOutputStream(100);
    set1.toDelta(out);
    assertThat(set1.hasDelta()).isFalse();
    ByteArrayDataInput in = new ByteArrayDataInput(out.toByteArray());
    RedisSet set2 = createRedisSet(1, 2);
    assertThat(set2).isNotEqualTo(set1);
    set2.fromDelta(in);
    assertThat(set2).isEqualTo(set1);
  }

  @Test
  public void setExpirationTimestamp_stores_delta_that_is_stable() throws IOException {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    RedisSet set1 = createRedisSet(1, 2);
    set1.setExpirationTimestamp(region, null, 999);
    assertThat(set1.hasDelta()).isTrue();
    HeapDataOutputStream out = new HeapDataOutputStream(100);
    set1.toDelta(out);
    assertThat(set1.hasDelta()).isFalse();
    ByteArrayDataInput in = new ByteArrayDataInput(out.toByteArray());
    RedisSet set2 = createRedisSet(1, 2);
    assertThat(set2).isNotEqualTo(set1);
    set2.fromDelta(in);
    assertThat(set2).isEqualTo(set1);
  }

  /************* test size of bytes in use *************/

  /******* constructor *******/
  @Test
  public void should_calculateSize_equalToROS_withNoMembers() {
    Set<byte[]> members = new HashSet<>();
    RedisSet set = new RedisSet(members);

    int expected = reflectionObjectSizer.sizeof(set);
    int actual = set.getSizeInBytes();

    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void should_calculateSize_equalToROS_withSingleMember() {
    Set<byte[]> members = new HashSet<>();
    members.add("value".getBytes());
    RedisSet set = new RedisSet(members);

    int actual = set.getSizeInBytes();
    int expected = reflectionObjectSizer.sizeof(set);

    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void should_calculateSize_equalToROS_withVaryingMemberCounts() {
    for (int i = 0; i < 1024; i += 16) {
      RedisSet set = createRedisSetOfSpecifiedSize(i);

      int expected = reflectionObjectSizer.sizeof(set);
      int actual = set.getSizeInBytes();

      assertThat(actual).isEqualTo(expected);
    }
  }

  @Test
  public void should_calculateSize_equalToROS_withVaryingMemberSize() {
    for (int i = 0; i < 1_600; i++) {
      RedisSet set = createRedisSetWithMemberOfSpecifiedSize(i * 64);
      int expected = reflectionObjectSizer.sizeof(set);
      int actual = set.getSizeInBytes();

      assertThat(actual).isEqualTo(expected);
    }
  }

  /******* sadd *******/
  @Test
  public void bytesInUse_sadd_withOneMember() {
    RedisSet set = new RedisSet(new ArrayList<>());
    Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    final RedisData returnData = mock(RedisData.class);
    when(region.put(any(RedisKey.class), any(RedisData.class))).thenReturn(returnData);
    final RedisKey key = new RedisKey("key".getBytes());
    String valueString = "value";

    final byte[] value = valueString.getBytes();
    List<byte[]> members = new ArrayList<>();
    members.add(value);

    set.sadd(members, region, key);

    set.clearDelta(); // because the region is mocked, the delta has to be explicitly cleared

    assertThat(set.getSizeInBytes()).isEqualTo(reflectionObjectSizer.sizeof(set));
  }

  @Test
  public void bytesInUse_sadd_withMultipleMembers() {
    RedisSet set = new RedisSet(new ArrayList<>());
    Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    final RedisData returnData = mock(RedisData.class);
    when(region.put(any(RedisKey.class), any(RedisData.class))).thenReturn(returnData);
    final RedisKey key = new RedisKey("key".getBytes());
    String baseString = "value";

    for (int i = 0; i < 1_000; i++) {
      List<byte[]> members = new ArrayList<>();
      String valueString = baseString + i;
      final byte[] value = valueString.getBytes();
      members.add(value);
      set.sadd(members, region, key);

      set.clearDelta(); // because the region is mocked, the delta has to be explicitly cleared

      long actual = set.getSizeInBytes();
      long expected = reflectionObjectSizer.sizeof(set);

      assertThat(actual).isEqualTo(expected);
    }
  }

  /******* remove *******/
  @Test
  public void size_shouldDecrease_WhenValueIsRemoved() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    final RedisData returnData = mock(RedisData.class);
    when(region.put(any(RedisKey.class), any(RedisData.class))).thenReturn(returnData);
    final RedisKey key = new RedisKey("key".getBytes());
    final byte[] value1 = "value1".getBytes();
    final byte[] value2 = "value2".getBytes();

    List<byte[]> members = new ArrayList<>();
    members.add(value1);
    members.add(value2);
    RedisSet set = new RedisSet(members);

    List<byte[]> membersToRemove = new ArrayList<>();
    membersToRemove.add(value1);
    set.srem(membersToRemove, region, key);

    set.clearDelta(); // because the region is mocked, the delta has to be explicitly cleared

    long finalSize = set.getSizeInBytes();
    long expectedSize = reflectionObjectSizer.sizeof(set);

    assertThat(finalSize).isEqualTo(expectedSize);
  }

  @Test
  public void remove_sizeShouldReturnToBaseOverhead_whenLastMemberIsRemoved() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    final RedisData returnData = mock(RedisData.class);
    when(region.put(any(RedisKey.class), any(RedisData.class))).thenReturn(returnData);
    final RedisKey key = new RedisKey("key".getBytes());
    final byte[] value = "value".getBytes();

    List<byte[]> members = new ArrayList<>();
    members.add(value);
    RedisSet set = new RedisSet(members);

    assertThat(set.getSizeInBytes()).isGreaterThan(BASE_REDIS_SET_OVERHEAD);

    set.srem(members, region, key);

    int finalSize = set.getSizeInBytes();

    assertThat(finalSize).isEqualTo(BASE_REDIS_SET_OVERHEAD);
  }

  /******** add and remove *******/
  @Test
  public void testSAddsAndSRems_changeSizeToMatchROSSize() {
    List<byte[]> initialMembers = new ArrayList<>();
    Random random = new Random(0);
    int length = 10;
    int numOfInitialMembers = 25;

    for (int i = 0; i < numOfInitialMembers; ++i) {
      byte[] data = new byte[length];
      random.nextBytes(data);
      initialMembers.add(data);
    }

    RedisSet set = new RedisSet(initialMembers);

    System.out.println("Adds, initial scard, size = " + set.scard() + "\t"
        + reflectionObjectSizer.sizeof(set));
    doAddsAndAssertSize(set, 100);

    System.out.println("Removes, initial scard, size = " + set.scard() + "\t"
        + reflectionObjectSizer.sizeof(set));
    doRemovesAndAssertSize(set, 175);

    System.out.println("Adds, initial scard, size = " + set.scard() + "\t"
        + reflectionObjectSizer.sizeof(set));
    doAddsAndAssertSize(set, 100);
  }

  /******** constants *******/
  // these tests contain the math that was used to derive the constants in RedisSet. If these tests
  // start failing, it is because the overhead of RedisSet has changed. If it has decreased, good
  // job! You can change the constants in RedisSet to reflect that. If it has increased, carefully
  // consider that increase before changing the constants.
  // Note: the per member overhead is known to not be constant. it changes as more members are
  // added, and/or as the members get longer
  @Test
  public void baseOverheadConstant_shouldMatchReflectedSize() {
    int baseRedisSetOverhead = reflectionObjectSizer.sizeof(new RedisSet(Collections.emptyList()));

    assertThat(baseRedisSetOverhead).isEqualTo(BASE_REDIS_SET_OVERHEAD);
  }

  @Test
  public void perMemberOverheadConstant_shouldMatchAverageMeasuredReflectedSize() {
    RedisSet set = new RedisSet(Collections.emptyList());

    // Used to compute the average per member overhead
    double totalOverhead = 0;
    final int totalMembers = 1000;

    // Generate pseudo-random data, but use fixed seed so the test is deterministic
    Random random = new Random(0);

    int addedMembers = 0;
    // Add 1000 members and compute the per member overhead after each add
    for (int i = 0; i < totalMembers; i++) {

      byte[] data = new byte[random.nextInt(30)];
      random.nextBytes(data);

      // Attempt to add a random member, but only increment addedMembers if the set was actually
      // updated. If the entry was already present, no overhead will be added
      if (!set.membersAdd(data)) {
        continue;
      }

      addedMembers++;

      // Compute the measured size
      int size = reflectionObjectSizer.sizeof(set);
      final int dataSize = data.length;

      // Compute per member overhead with this number of members
      int overHeadPerMember = (size - BASE_REDIS_SET_OVERHEAD - dataSize) / addedMembers;
      totalOverhead += overHeadPerMember;
    }

    // Assert that the average overhead matches the constant
    long averageOverhead = Math.round(totalOverhead / addedMembers);
    assertThat(PER_MEMBER_OVERHEAD).isEqualTo(averageOverhead);
  }

  /******* tests of RedisSet's helper methods *******/
  @Test
  public void testLogBaseTwo() {
    int actual;
    int calculated;
    for (int i = 1; i < 65; ++i) {
      actual = (int) (Math.log(i) / Math.log(2));
      calculated = logBaseTwo(i);
      assertThat(actual).isEqualTo(calculated);
    }
  }

  @Test
  public void testLogBaseTwo_ThrowsWhenArgumentIsLessThanOne() {
    assertThatThrownBy(() -> logBaseTwo(0)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> logBaseTwo(-1)).isInstanceOf(IllegalArgumentException.class);
  }

  /******* helper methods *******/
  private RedisSet createRedisSetOfSpecifiedSize(int setSize) {
    List<byte[]> arrayList = new ArrayList<>();
    for (int i = 0; i < setSize; i++) {
      arrayList.add(("abcdefgh" + i).getBytes());
    }
    return new RedisSet(arrayList);
  }

  private RedisSet createRedisSetWithMemberOfSpecifiedSize(int memberSize) {
    List<byte[]> arrayList = new ArrayList<>();
    byte[] member = createMemberOfSpecifiedSize("a", memberSize).getBytes();
    if (member.length > 0) {
      arrayList.add(member);
    }
    return new RedisSet(arrayList);
  }

  private String createMemberOfSpecifiedSize(final String base, final int stringSize) {
    Random random = new Random();
    if (base.length() > stringSize) {
      return "";
    }
    StringBuilder sb = new StringBuilder(stringSize);
    sb.append(base);
    for (int i = base.length(); i < stringSize; i++) {
      int randy = random.nextInt(10);
      sb.append(randy);
    }
    return sb.toString();
  }

  void doAddsAndAssertSize(RedisSet set, int membersToAdd) {
    int length;
    Random random = new Random(0);

    System.out.println("entries\tcalcOH\trszOH\trealOH\tcap\tmbmOH");

    for (int i = 0; i <= membersToAdd; ++i) {
      length = random.nextInt(20);
      if (length < 2) {
        length++;
      }

      int initialSize = reflectionObjectSizer.sizeof(set);
      int initialCalculatedSize = set.getSizeInBytes();
      int batchSize = random.nextInt(4);
      int memberOverhead = 0;

      for (int j = 0; j < batchSize; ++j) {
        byte[] data = new byte[length];
        random.nextBytes(data);
        set.membersAdd(data);
        memberOverhead += 8 * (2 + (length - 1) / 8);
      }

      int actualOverhead = reflectionObjectSizer.sizeof(set) - initialSize;
      int resizeOH = actualOverhead - memberOverhead;
      int calculatedOH = set.getSizeInBytes() - initialCalculatedSize;

      System.out.println(set.scard() + "\t" + calculatedOH + "\t" + resizeOH + "\t"
          + actualOverhead + "\t" + set.backingSetCapacity + "\t" + memberOverhead);

      assertThat(calculatedOH).isEqualTo(actualOverhead);
    }
  }

  void doRemovesAndAssertSize(RedisSet set, int membersToRemove) {
    int length;
    Random random = new Random(0);

    System.out.println("entries\tcalcOH\trszOH\trealOH\tcap\tmbmOH");

    for (int i = 0; i <= membersToRemove; ++i) {
      length = random.nextInt(20);
      if (length < 2) {
        length++;
      }

      int initialSize = reflectionObjectSizer.sizeof(set);
      int initialCalculatedSize = set.getSizeInBytes();
      int batchSize = random.nextInt(4);
      int memberOverhead = 0;

      for (int j = 0; j < batchSize; ++j) {
        byte[] data = new byte[length];
        random.nextBytes(data);
        set.membersRemove(data);
        memberOverhead -= 8 * (2 + (length - 1) / 8);
      }

      int actualOverhead = reflectionObjectSizer.sizeof(set) - initialSize;
      int resizeOH = actualOverhead - memberOverhead;
      int calculatedOH = set.getSizeInBytes() - initialCalculatedSize;

      System.out.println(set.scard() + "\t" + calculatedOH + "\t" + resizeOH + "\t" + actualOverhead
          + "\t" + set.backingSetCapacity + "\t" + memberOverhead);

      assertThat(calculatedOH).isEqualTo(actualOverhead);
    }
  }
}
