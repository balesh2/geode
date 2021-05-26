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

package org.apache.geode.redis.internal.executor.sortedset;

import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;
import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertExactNumberOfArgs;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.redis.internal.RedisConstants;

public abstract class AbstractZIncrByIntegrationTest implements RedisIntegrationTest {
  JedisCluster jedis;

  @Before
  public void setUp() {
    jedis = new JedisCluster(new HostAndPort("localhost", getPort()), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    flushAll();
    jedis.close();
  }

  @Test
  public void zIncrByErrors_givenWrongKeyType() {
    final String STRING_KEY = "stringKey";
    jedis.set(STRING_KEY, "value");
    assertThatThrownBy(
        () -> jedis.sendCommand(STRING_KEY, Protocol.Command.ZINCRBY, STRING_KEY, "1", "member"))
            .hasMessageContaining("WRONGTYPE " + RedisConstants.ERROR_WRONG_TYPE);
  }

  @Test
  public void zIncrByErrors_givenWrongNumberOfArguments() {
    final String key = "key";
    final byte[] member = "member".getBytes();
    jedis.zadd(key.getBytes(), 0.0, member);

    assertExactNumberOfArgs(jedis, Protocol.Command.ZINCRBY, 3);
  }

  @Test
  public void shouldError_givenNonFloatIncrement() {
    String key = "key";
    String member = "member";
    String nonFloatIncrement = "q";
    jedis.zadd(key, 1.0, member);

    assertThatThrownBy(
        () -> jedis.sendCommand(key, Protocol.Command.ZINCRBY, key, nonFloatIncrement, member))
            .hasMessageContaining(RedisConstants.ERROR_NOT_A_VALID_FLOAT);
  }

  @Test
  public void shouldSetScoreToInfinity_givenInfiniteIncrement() {
    String key = "key";
    String member1 = "member1";
    String member2 = "member2";

    jedis.zadd(key, 1.0, member1);
    assertThat(jedis.zincrby(key, POSITIVE_INFINITY, member1)).isEqualTo(POSITIVE_INFINITY);

    jedis.zadd(key, 1.0, member2);
    assertThat(jedis.zincrby(key, NEGATIVE_INFINITY, member2)).isEqualTo(NEGATIVE_INFINITY);
  }

  @Test
  public void zIncrByCreatesNewKey_whenIncrementedKeyDoesNotExist() {
    final byte[] key = "key".getBytes();
    final byte[] member = "member".getBytes();
    final double increment = 1.5;

    assertThat(jedis.zincrby(key, increment, member)).isEqualTo(increment);
    assertThat(jedis.zscore(key, member)).isEqualTo(increment);
  }

  @Test
  public void zIncrByCreatesNewMember_whenIncrementedMemberDoesNotExist() {
    final byte[] key = "key".getBytes();
    final byte[] member = "member".getBytes();
    final double increment = 1.5;

    jedis.zadd(key, increment, "something".getBytes());

    assertThat(jedis.zincrby(key, increment, member)).isEqualTo(increment);
    assertThat(jedis.zscore(key, member)).isEqualTo(increment);
  }

  @Test
  public void shouldIncrementScore_givenKeyAndMemberExist() {
    testZIncrByIncrements("key".getBytes(), "member".getBytes(), 2.7, 1.5);
  }

  @Test
  public void shouldIncrementScore_givenNegativeIncrement() {
    testZIncrByIncrements("key".getBytes(), "member".getBytes(), 2.7, -1.5);
  }

  @Test
  public void shouldIncrementScoreToANegativeNumber_givenNegativeIncrementGreaterThanScore() {
    testZIncrByIncrements("key".getBytes(), "member".getBytes(), 2.7, -5.6);
  }

  @Test
  public void shouldIncrementScore_givenNegativeInitialScoreAndPositiveIncrement() {
    testZIncrByIncrements("key".getBytes(), "member".getBytes(), -2.7, 1.7);
  }

  @Test
  public void shouldIncrementScore_givenIncrementInExponentialForm() {
    testZIncrByIncrements("key".getBytes(), "member".getBytes(), -2.7, 2e5);
    testZIncrByIncrements("key".getBytes(), "member".getBytes(), 2e5, -2.7);
  }

  private void testZIncrByIncrements(final byte[] key, final byte[] member,
      final double initialScore, final double increment) {
    jedis.zadd(key, initialScore, member);
    assertThat(jedis.zincrby(key, increment, member)).isEqualTo(initialScore + increment);
    assertThat(jedis.zscore(key, member)).isEqualTo(initialScore + increment);
  }
}
