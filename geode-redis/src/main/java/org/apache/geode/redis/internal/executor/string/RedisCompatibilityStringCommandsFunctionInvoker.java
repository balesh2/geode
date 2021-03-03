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
package org.apache.geode.redis.internal.executor.string;

import static org.apache.geode.redis.internal.RedisCompatibilityCommandType.APPEND;
import static org.apache.geode.redis.internal.RedisCompatibilityCommandType.BITCOUNT;
import static org.apache.geode.redis.internal.RedisCompatibilityCommandType.BITOP;
import static org.apache.geode.redis.internal.RedisCompatibilityCommandType.BITPOS;
import static org.apache.geode.redis.internal.RedisCompatibilityCommandType.DECR;
import static org.apache.geode.redis.internal.RedisCompatibilityCommandType.DECRBY;
import static org.apache.geode.redis.internal.RedisCompatibilityCommandType.GET;
import static org.apache.geode.redis.internal.RedisCompatibilityCommandType.GETBIT;
import static org.apache.geode.redis.internal.RedisCompatibilityCommandType.GETRANGE;
import static org.apache.geode.redis.internal.RedisCompatibilityCommandType.GETSET;
import static org.apache.geode.redis.internal.RedisCompatibilityCommandType.INCR;
import static org.apache.geode.redis.internal.RedisCompatibilityCommandType.INCRBY;
import static org.apache.geode.redis.internal.RedisCompatibilityCommandType.INCRBYFLOAT;
import static org.apache.geode.redis.internal.RedisCompatibilityCommandType.MGET;
import static org.apache.geode.redis.internal.RedisCompatibilityCommandType.SET;
import static org.apache.geode.redis.internal.RedisCompatibilityCommandType.SETBIT;
import static org.apache.geode.redis.internal.RedisCompatibilityCommandType.SETRANGE;
import static org.apache.geode.redis.internal.RedisCompatibilityCommandType.STRLEN;

import java.math.BigDecimal;
import java.util.List;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.data.ByteArrayWrapper;
import org.apache.geode.redis.internal.data.RedisCompatibilityData;
import org.apache.geode.redis.internal.executor.RedisCompatibilityCommandsFunctionInvoker;

/**
 * This class is used by netty redis string command executors
 * to invoke a geode function that will run on a
 * particular server to do the redis command.
 */
public class RedisCompatibilityStringCommandsFunctionInvoker extends
    RedisCompatibilityCommandsFunctionInvoker
    implements RedisCompatibilityStringCommands {

  public RedisCompatibilityStringCommandsFunctionInvoker(
      Region<ByteArrayWrapper, RedisCompatibilityData> region) {
    super(region);
  }

  @Override
  public long append(ByteArrayWrapper key, ByteArrayWrapper valueToAppend) {
    return invokeCommandFunction(key, APPEND, valueToAppend);
  }

  @Override
  public ByteArrayWrapper get(ByteArrayWrapper key) {
    return invokeCommandFunction(key, GET);
  }

  @Override
  public boolean set(ByteArrayWrapper key, ByteArrayWrapper value, SetOptions options) {
    return invokeCommandFunction(key, SET, value, options);
  }

  @Override
  public long incr(ByteArrayWrapper key) {
    return invokeCommandFunction(key, INCR);
  }

  @Override
  public long decr(ByteArrayWrapper key) {
    return invokeCommandFunction(key, DECR);
  }

  @Override
  public ByteArrayWrapper getset(ByteArrayWrapper key, ByteArrayWrapper value) {
    return invokeCommandFunction(key, GETSET, value);
  }

  @Override
  public long incrby(ByteArrayWrapper key, long increment) {
    return invokeCommandFunction(key, INCRBY, increment);
  }

  @Override
  public long decrby(ByteArrayWrapper key, long decrement) {
    return invokeCommandFunction(key, DECRBY, decrement);
  }

  @Override
  public ByteArrayWrapper getrange(ByteArrayWrapper key, long start, long end) {
    return invokeCommandFunction(key, GETRANGE, start, end);
  }

  @Override
  public long bitcount(ByteArrayWrapper key, int start, int end) {
    return invokeCommandFunction(key, BITCOUNT, start, end);
  }

  @Override
  public long bitcount(ByteArrayWrapper key) {
    return invokeCommandFunction(key, BITCOUNT);
  }

  @Override
  public int strlen(ByteArrayWrapper key) {
    return invokeCommandFunction(key, STRLEN);
  }

  @Override
  public int getbit(ByteArrayWrapper key, int offset) {
    return invokeCommandFunction(key, GETBIT, offset);
  }

  @Override
  public int setbit(ByteArrayWrapper key, long offset, int value) {
    return invokeCommandFunction(key, SETBIT, offset, value);
  }

  @Override
  public BigDecimal incrbyfloat(ByteArrayWrapper key, BigDecimal increment) {
    return invokeCommandFunction(key, INCRBYFLOAT, increment);
  }

  @Override
  public int bitop(String operation, ByteArrayWrapper destKey, List<ByteArrayWrapper> sources) {
    return invokeCommandFunction(destKey, BITOP, operation, sources);
  }

  @Override
  public int bitpos(ByteArrayWrapper key, int bit, int start, Integer end) {
    return invokeCommandFunction(key, BITPOS, bit, start, end);
  }

  @Override
  public int setrange(ByteArrayWrapper key, int offset, byte[] value) {
    return invokeCommandFunction(key, SETRANGE, offset, value);
  }

  @Override
  public ByteArrayWrapper mget(ByteArrayWrapper key) {
    return invokeCommandFunction(key, MGET);
  }
}
