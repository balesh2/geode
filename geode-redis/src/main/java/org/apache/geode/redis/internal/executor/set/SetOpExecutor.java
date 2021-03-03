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
package org.apache.geode.redis.internal.executor.set;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.geode.redis.internal.data.ByteArrayWrapper;
import org.apache.geode.redis.internal.executor.RedisCompatibilityResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public abstract class SetOpExecutor extends SetExecutor {

  @Override
  public RedisCompatibilityResponse executeCommand(Command command,
      ExecutionHandlerContext context) {
    int setsStartIndex = 1;

    if (isStorage()) {
      setsStartIndex++;
    }

    List<ByteArrayWrapper> commandElements = command.getProcessedCommandWrappers();
    ArrayList<ByteArrayWrapper> setKeys =
        new ArrayList<>(commandElements.subList(setsStartIndex, commandElements.size()));
    if (isStorage()) {
      ByteArrayWrapper destination = command.getKey();
      RedisCompatibilitySetCommands redisCompatibilitySetCommands = createRedisSetCommands(context);
      int storeCount;
      switch (command.getCommandType()) {
        case SUNIONSTORE:
          storeCount = redisCompatibilitySetCommands.sunionstore(destination, setKeys);
          break;
        case SINTERSTORE:
          storeCount = redisCompatibilitySetCommands.sinterstore(destination, setKeys);
          break;
        case SDIFFSTORE:
          storeCount = redisCompatibilitySetCommands.sdiffstore(destination, setKeys);
          break;
        default:
          throw new IllegalStateException(
              "expected a set store command but found: " + command.getCommandType());
      }
      return RedisCompatibilityResponse.integer(storeCount);
    } else {
      return doActualSetOperation(context, setKeys);
    }
  }

  private RedisCompatibilityResponse doActualSetOperation(ExecutionHandlerContext context,
      ArrayList<ByteArrayWrapper> setKeys) {
    RedisCompatibilitySetCommands redisCompatibilitySetCommands = createRedisSetCommands(context);
    ByteArrayWrapper firstSetKey = setKeys.remove(0);
    Set<ByteArrayWrapper> resultSet = redisCompatibilitySetCommands.smembers(firstSetKey);

    for (ByteArrayWrapper key : setKeys) {
      Set<ByteArrayWrapper> nextSet = redisCompatibilitySetCommands.smembers(key);
      if (doSetOp(resultSet, nextSet)) {
        break;
      }
    }

    if (resultSet.isEmpty()) {
      return RedisCompatibilityResponse.emptyArray();
    } else {
      return respondBulkStrings(resultSet);
    }
  }

  /**
   * @return true if no further calls of doSetOp are needed
   */
  protected abstract boolean doSetOp(Set<ByteArrayWrapper> resultSet,
      Set<ByteArrayWrapper> nextSet);

  protected abstract boolean isStorage();

}
