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

import java.math.BigDecimal;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang3.tuple.Pair;

import org.apache.geode.redis.internal.RedisCompatibilityConstants;
import org.apache.geode.redis.internal.data.ByteArrayWrapper;
import org.apache.geode.redis.internal.executor.RedisCompatibilityResponse;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class IncrByFloatExecutor extends StringExecutor {

  private static final int INCREMENT_INDEX = 2;

  private static final Pattern invalidArgs =
      Pattern.compile("[+-]?(inf|infinity)", Pattern.CASE_INSENSITIVE);

  @Override
  public RedisCompatibilityResponse executeCommand(Command command,
      ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();
    ByteArrayWrapper key = command.getKey();

    Pair<BigDecimal, RedisCompatibilityResponse> validated =
        validateIncrByFloatArgument(commandElems.get(INCREMENT_INDEX));
    if (validated.getRight() != null) {
      return validated.getRight();
    }

    RedisCompatibilityStringCommands stringCommands = getRedisStringCommands(context);
    BigDecimal result = stringCommands.incrbyfloat(key, validated.getLeft());

    return RedisCompatibilityResponse.bigDecimal(result);
  }

  public static Pair<BigDecimal, RedisCompatibilityResponse> validateIncrByFloatArgument(
      byte[] incrArray) {
    String doub = Coder.bytesToString(incrArray).toLowerCase();
    if (invalidArgs.matcher(doub).matches()) {
      return Pair.of(null,
          RedisCompatibilityResponse.error(RedisCompatibilityConstants.ERROR_NAN_OR_INFINITY));
    }

    BigDecimal increment;
    try {
      increment = Coder.bytesToBigDecimal(incrArray);
    } catch (NumberFormatException e) {
      return Pair.of(null,
          RedisCompatibilityResponse.error(RedisCompatibilityConstants.ERROR_NOT_A_VALID_FLOAT));
    }

    return Pair.of(increment, null);
  }
}
