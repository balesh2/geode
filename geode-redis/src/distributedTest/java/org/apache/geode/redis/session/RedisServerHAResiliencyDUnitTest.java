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
 */

package org.apache.geode.redis.session;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import redis.clients.jedis.Jedis;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.internal.data.ByteArrayWrapper;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.junit.categories.RedisTest;

@Category({RedisTest.class})
public class RedisServerHAResiliencyDUnitTest {
  private static final Logger log = LogService.getLogger();

  private static String LOCALHOST = "localhost";
  public static final String KEY = "redis_key";
  public static final String VALUE = "a_value";

  private static MemberVM locator;
  private static MemberVM server1;
  private static MemberVM server2;
  private static MemberVM server3;

  private static final int NUM_REDIS_SERVERS = 3;
  private static final int JEDIS_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  private static final int RETRIEVE_REDIS_VALUE_TIMEOUT = 15;

  private Jedis[] jedisConnections;
  Map<String, Integer> vmMap;

  @ClassRule
  public static RedisClusterStartupRule cluster = new RedisClusterStartupRule(4);

  @BeforeClass
  public static void setup() {
    locator = cluster.startLocatorVM(3);

    server1 = cluster.startRedisVM(0, locator.getPort());
    server2 = cluster.startRedisVM(1, locator.getPort());
    server3 = cluster.startRedisVM(2, locator.getPort());
  }

  @Before
  public void setupTest() {
    jedisConnections = new Jedis[NUM_REDIS_SERVERS];
    for (int i = 0; i< NUM_REDIS_SERVERS; i++) {
      jedisConnections[i] = new Jedis(LOCALHOST, cluster.getRedisPort(i), JEDIS_TIMEOUT);
    }
    jedisConnections[0].flushAll();
    // set a key-value entry
    jedisConnections[0].set(KEY, VALUE);

    // pull from another server to confirm it got set
    String retVal = jedisConnections[2].get(KEY);

    assertThat(retVal).isEqualTo(VALUE);

    vmMap = new HashMap<>();
    vmMap.put(server1.getName(), 0);
    vmMap.put(server2.getName(), 1);
    vmMap.put(server3.getName(), 2);
  }

  @Test
  public void killingPrimaryServer_dataStillAccessible() {
    // find the primary and secondary for that key
    String memberForPrimary = server1.invoke(() -> {
      InternalCache cache = RedisClusterStartupRule.getCache();
      Region<ByteArrayWrapper, ByteArrayWrapper> region = cache.getRegion("/__REDIS_DATA");

      DistributedMember distributedMember = PartitionRegionHelper
          .getPrimaryMemberForKey(region, new ByteArrayWrapper(KEY.getBytes()));

      AtomicReference<String> currentPrimary =
          new AtomicReference<>(distributedMember
              .getName());

      return currentPrimary.get();
    });

    log.info("primary server for key is: " + memberForPrimary);


    int serverIndex = vmMap.get(memberForPrimary);
    cluster.crashVM(serverIndex);
    cluster.startRedisVM(serverIndex, locator.getPort());

    final int jedisIndex = (serverIndex+1)%2; // connect to non-crashed server
    GeodeAwaitility.await().ignoreExceptions().atMost(RETRIEVE_REDIS_VALUE_TIMEOUT, TimeUnit.SECONDS)
        .untilAsserted(() ->assertThat(jedisConnections[jedisIndex].get(KEY)).isEqualTo(VALUE));
  }

  @Test
  public void killingSecondaryServer_dataStillAccessible() {
    String secondaryMember = server1.invoke(() -> {
      InternalCache cache = RedisClusterStartupRule.getCache();
      Region<ByteArrayWrapper, ByteArrayWrapper> region = cache.getRegion("/__REDIS_DATA");

      Set<DistributedMember> distributedMemberSet = PartitionRegionHelper
          .getRedundantMembersForKey(region, new ByteArrayWrapper(KEY.getBytes()));
      DistributedMember[] distributedMembers =
          distributedMemberSet.toArray(new DistributedMember[distributedMemberSet.size()]);
      return distributedMembers[0].getName();
    });

    int serverIndex = vmMap.get(secondaryMember);
    cluster.crashVM(serverIndex);
    cluster.startRedisVM(serverIndex, locator.getPort());

    final int jedisIndex = (serverIndex+1)%2; // connect to non-crashed server
    GeodeAwaitility.await().ignoreExceptions().atMost(RETRIEVE_REDIS_VALUE_TIMEOUT, TimeUnit.SECONDS)
        .untilAsserted(() ->assertThat(jedisConnections[jedisIndex].get(KEY)).isEqualTo(VALUE));
  }

  @Test
  public void killingPrimaryThenSecondary_withLongerInterval_maintainsRedundancy() {
    String secondaryMember = server1.invoke(() -> {
      InternalCache cache = RedisClusterStartupRule.getCache();
      Region<ByteArrayWrapper, ByteArrayWrapper> region = cache.getRegion("/__REDIS_DATA");

      Set<DistributedMember> distributedMemberSet = PartitionRegionHelper
          .getRedundantMembersForKey(region, new ByteArrayWrapper(KEY.getBytes()));
      DistributedMember[] distributedMembers =
          distributedMemberSet.toArray(new DistributedMember[distributedMemberSet.size()]);
      return distributedMembers[0].getName();
    });

    String primaryMember = server1.invoke(() -> {
      InternalCache cache = RedisClusterStartupRule.getCache();
      Region<ByteArrayWrapper, ByteArrayWrapper> region = cache.getRegion("/__REDIS_DATA");

      DistributedMember secondaryDistributedMember = PartitionRegionHelper
          .getPrimaryMemberForKey(region, new ByteArrayWrapper(KEY.getBytes()));

      AtomicReference<String> currentPrimary =
          new AtomicReference<>(secondaryDistributedMember
              .getName());

      return currentPrimary.get();
    });

    assertThat(primaryMember).isNotEqualTo(secondaryMember);
    final int primaryServerIndex = vmMap.get(primaryMember);
    final int secondaryServerIndex = vmMap.get(secondaryMember);
    final int jedisIndex = NUM_REDIS_SERVERS - (primaryServerIndex + secondaryServerIndex);
    assertThat(jedisIndex).isNotEqualTo(primaryServerIndex);
    assertThat(jedisIndex).isNotEqualTo(secondaryServerIndex);

    // kill & restart primary
    cluster.crashVM(primaryServerIndex);
    cluster.startRedisVM(primaryServerIndex, locator.getPort());
    cluster.getMember(primaryServerIndex).waitTilFullyReconnected();

    // kill & restart secondary
    cluster.crashVM(secondaryServerIndex);
    cluster.startRedisVM(secondaryServerIndex, locator.getPort());
    cluster.getMember(secondaryServerIndex).waitTilFullyReconnected();

    //index of new server to query = sum of all indices - primary -secondary
    // can still get data
    GeodeAwaitility.await().ignoreExceptions().atMost(RETRIEVE_REDIS_VALUE_TIMEOUT, TimeUnit.SECONDS)
        .untilAsserted(() ->assertThat(jedisConnections[jedisIndex].get(KEY)).isEqualTo(VALUE));
  }

  @Test
  public void killingPrimaryThenSecondary_withShorterInterval_messesThingsUp() {
    String secondaryMember = server1.invoke(() -> {
      InternalCache cache = RedisClusterStartupRule.getCache();
      Region<ByteArrayWrapper, ByteArrayWrapper> region = cache.getRegion("/__REDIS_DATA");

      Set<DistributedMember> distributedMemberSet = PartitionRegionHelper
          .getRedundantMembersForKey(region, new ByteArrayWrapper(KEY.getBytes()));
      if (distributedMemberSet.size() > 1) {
        throw new RuntimeException("Hey, there should only be one redundant member!");
      }
      DistributedMember[] distributedMembers =
          distributedMemberSet.toArray(new DistributedMember[distributedMemberSet.size()]);
      return distributedMembers[0].getName();
    });

    String primaryMember = server1.invoke(() -> {
      InternalCache cache = RedisClusterStartupRule.getCache();
      Region<ByteArrayWrapper, ByteArrayWrapper> region = cache.getRegion("/__REDIS_DATA");

      DistributedMember secondaryDistributedMember = PartitionRegionHelper
          .getPrimaryMemberForKey(region, new ByteArrayWrapper(KEY.getBytes()));

      AtomicReference<String> currentPrimary =
          new AtomicReference<>(secondaryDistributedMember
              .getName());

      return currentPrimary.get();
    });

    assertThat(primaryMember).isNotEqualTo(secondaryMember);
    final int primaryServerIndex = vmMap.get(primaryMember);
    final int secondaryServerIndex = vmMap.get(secondaryMember);
    final int jedisIndex = NUM_REDIS_SERVERS - (primaryServerIndex + secondaryServerIndex);
    assertThat(jedisIndex).isNotEqualTo(primaryServerIndex);
    assertThat(jedisIndex).isNotEqualTo(secondaryServerIndex);

    // kill & restart primary
    cluster.crashVM(primaryServerIndex);
    cluster.startRedisVM(primaryServerIndex, locator.getPort());
//    cluster.getMember(primaryServerIndex).waitTilFullyReconnected();

    // kill & restart secondary
    cluster.crashVM(secondaryServerIndex);
    cluster.startRedisVM(secondaryServerIndex, locator.getPort());
//    cluster.getMember(secondaryServerIndex).waitTilFullyReconnected();

    // can still get data
    GeodeAwaitility.await().atMost(RETRIEVE_REDIS_VALUE_TIMEOUT, TimeUnit.SECONDS)
        .untilAsserted(() ->assertThat(jedisConnections[jedisIndex].get(KEY)).isEqualTo(VALUE));

  }

}
