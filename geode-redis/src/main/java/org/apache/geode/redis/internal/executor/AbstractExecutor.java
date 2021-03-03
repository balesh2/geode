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
package org.apache.geode.redis.internal.executor;

import java.util.Collection;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.RedisCompatibilityServer;
import org.apache.geode.redis.internal.data.ByteArrayWrapper;
import org.apache.geode.redis.internal.data.RedisCompatibilityData;
import org.apache.geode.redis.internal.executor.key.RedisCompatibilityKeyCommands;
import org.apache.geode.redis.internal.executor.key.RedisCompatibilityKeyCommandsFunctionInvoker;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

/**
 * The AbstractExecutor is the base of all {@link Executor} types for the
 * {@link RedisCompatibilityServer}.
 */
public abstract class AbstractExecutor implements Executor {

  protected RedisCompatibilityResponse respondBulkStrings(Object message) {
    if (message instanceof Collection) {
      return RedisCompatibilityResponse.array((Collection<?>) message);
    } else {
      return RedisCompatibilityResponse.bulkString(message);
    }
  }

  protected RedisCompatibilityKeyCommands getRedisKeyCommands(ExecutionHandlerContext context) {
    return new RedisCompatibilityKeyCommandsFunctionInvoker(
        context.getRegionProvider().getDataRegion());
  }

  protected Region<ByteArrayWrapper, RedisCompatibilityData> getDataRegion(
      ExecutionHandlerContext context) {
    return context.getRegionProvider().getDataRegion();
  }
}
