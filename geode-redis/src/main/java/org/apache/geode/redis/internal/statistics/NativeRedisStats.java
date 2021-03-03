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

package org.apache.geode.redis.internal.statistics;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.internal.statistics.StatisticsClockFactory.getTime;
import static org.apache.geode.logging.internal.executors.LoggingExecutors.newSingleThreadScheduledExecutor;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.redis.internal.RedisCompatibilityCommandType;

public class NativeRedisStats {
  private final AtomicLong commandsProcessed = new AtomicLong();
  private final AtomicLong totalNetworkBytesRead = new AtomicLong();
  private final AtomicLong totalConnectionsReceived = new AtomicLong();
  private final AtomicLong currentlyConnectedClients = new AtomicLong();
  private final AtomicLong expirations = new AtomicLong();
  private final AtomicLong keyspaceHits = new AtomicLong();
  private final AtomicLong keyspaceMisses = new AtomicLong();
  private final ScheduledExecutorService perSecondExecutor;
  private volatile double networkKiloBytesReadOverLastSecond;
  private volatile long opsPerformedLastTick;
  private double opsPerformedOverLastSecond;
  private long previousNetworkBytesRead;
  private final StatisticsClock clock;
  private final RedisCompatibilityStats redisCompatibilityStats;
  private final long START_TIME_IN_NANOS;


  public NativeRedisStats(StatisticsClock clock,
      RedisCompatibilityStats redisCompatibilityStats) {

    this.clock = clock;
    this.redisCompatibilityStats = redisCompatibilityStats;
    perSecondExecutor = startPerSecondUpdater();
    START_TIME_IN_NANOS = clock.getTime();
  }

  public void incCommandsProcessed() {
    commandsProcessed.incrementAndGet();
    redisCompatibilityStats.incrementCommandsProcessed();
  }

  public long getCommandsProcessed() {
    return commandsProcessed.get();
  }

  public void addClient() {
    totalConnectionsReceived.incrementAndGet();
    currentlyConnectedClients.incrementAndGet();
    redisCompatibilityStats.addClient();
  }

  public void removeClient() {
    currentlyConnectedClients.decrementAndGet();
    redisCompatibilityStats.removeClient();
  }

  public long getTotalConnectionsReceived() {
    return totalConnectionsReceived.get();
  }

  public long getConnectedClients() {
    return currentlyConnectedClients.get();
  }

  public double getNetworkKiloBytesReadOverLastSecond() {
    return networkKiloBytesReadOverLastSecond;
  }

  public double getOpsPerformedOverLastSecond() {
    return opsPerformedOverLastSecond;
  }

  public long startCommand() {
    return getTime();
  }

  public void endCommand(RedisCompatibilityCommandType command, long start) {
    redisCompatibilityStats.endCommand(command, start);
  }

  public void incNetworkBytesRead(long bytesRead) {
    totalNetworkBytesRead.addAndGet(bytesRead);
    redisCompatibilityStats.incrementTotalNetworkBytesRead(bytesRead);
  }

  public long getTotalNetworkBytesRead() {
    return totalNetworkBytesRead.get();
  }

  private long getCurrentTimeNanos() {
    return clock.getTime();
  }

  public long getUptimeInMilliseconds() {
    long uptimeInNanos = getCurrentTimeNanos() - START_TIME_IN_NANOS;
    return TimeUnit.NANOSECONDS.toMillis(uptimeInNanos);
  }

  public long getUptimeInSeconds() {
    return TimeUnit.MILLISECONDS.toSeconds(getUptimeInMilliseconds());
  }

  public long getUptimeInDays() {
    return TimeUnit.MILLISECONDS.toDays(getUptimeInMilliseconds());
  }

  public void incKeyspaceHits() {
    keyspaceHits.incrementAndGet();
    redisCompatibilityStats.incrementKeyspaceHits();
  }

  public long getKeyspaceHits() {
    return keyspaceHits.get();
  }

  public void incKeyspaceMisses() {
    keyspaceMisses.incrementAndGet();
    redisCompatibilityStats.incrementKeyspaceMisses();
  }

  public long getKeyspaceMisses() {
    return keyspaceMisses.get();
  }

  public long startPassiveExpirationCheck() {
    return getCurrentTimeNanos();
  }

  public void endPassiveExpirationCheck(long start, long expireCount) {
    redisCompatibilityStats.endPassiveExpirationCheck(start, expireCount);
  }

  public long startExpiration() {
    return getCurrentTimeNanos();
  }

  public void endExpiration(long start) {
    redisCompatibilityStats.endExpiration(start);
    expirations.incrementAndGet();
  }

  public void close() {
    redisCompatibilityStats.close();
    stopPerSecondUpdater();
  }

  private ScheduledExecutorService startPerSecondUpdater() {
    int INTERVAL = 1;

    ScheduledExecutorService perSecondExecutor =
        newSingleThreadScheduledExecutor("GemFireRedis-PerSecondUpdater-");

    perSecondExecutor.scheduleWithFixedDelay(
        () -> doPerSecondUpdates(),
        INTERVAL,
        INTERVAL,
        SECONDS);

    return perSecondExecutor;
  }

  private void stopPerSecondUpdater() {
    perSecondExecutor.shutdownNow();
  }

  private void doPerSecondUpdates() {
    updateNetworkKilobytesReadLastSecond();
    updateOpsPerformedOverLastSecond();
  }

  private void updateNetworkKilobytesReadLastSecond() {
    long totalNetworkBytesRead = getTotalNetworkBytesRead();
    long deltaNetworkBytesRead = totalNetworkBytesRead - previousNetworkBytesRead;
    networkKiloBytesReadOverLastSecond = deltaNetworkBytesRead / 1000;
    previousNetworkBytesRead = getTotalNetworkBytesRead();
  }

  private void updateOpsPerformedOverLastSecond() {
    long totalOpsPerformed = getCommandsProcessed();
    opsPerformedOverLastSecond = totalOpsPerformed - opsPerformedLastTick;
    opsPerformedLastTick = getCommandsProcessed();
  }
}
