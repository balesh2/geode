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

import static org.apache.geode.distributed.ConfigurationProperties.REDIS_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.REDIS_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.REDIS_PORT;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.session.SessionRepository;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import org.apache.geode.internal.tcp.ConnectionException;
import org.apache.geode.redis.internal.GeodeRedisServer;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.junit.categories.RedisTest;
import org.apache.geode.test.junit.rules.ConcurrencyRule;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

@Category({RedisTest.class})
public class RedisServerHAResiliencyDUnitTest extends SessionDUnitTest {
  private static final String LOCAL_HOST = "127.0.0.1";
  private static final int JEDIS_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  private static final int numServers = 4;
  private static final int numClients = 8;
  private static final int numStops = 5;

  private static int serverPortCounter = 0;

  MemberVM[] servers = new MemberVM[numServers];
  Jedis[] clients = new Jedis[numClients];

  protected AnnotationConfigApplicationContext ctx;
  protected SessionRepository sessionRepository;

  private Properties properties = new Properties();

  @Rule
  public ExecutorServiceRule executor = new ExecutorServiceRule();

  @Rule
  public static RedisClusterStartupRule cluster = new RedisClusterStartupRule();

  @Rule
  ConcurrencyRule concurrencyRule = new ConcurrencyRule();

  @BeforeClass
  public static void setupClass() {
    SessionDUnitTest.setup();
    setupSpringApps(DEFAULT_SESSION_TIMEOUT);
  }

  @Test
  public void idk_doSomething() {
    properties.setProperty(REDIS_BIND_ADDRESS, "localhost");
    properties.setProperty(REDIS_PORT, "0");
    properties.setProperty(REDIS_ENABLED, "true");
    properties.setProperty(GeodeRedisServer.ENABLE_REDIS_UNSUPPORTED_COMMANDS_PARAM, "true");

    MemberVM locator0 = cluster.startLocatorVM(0, properties, 0);

    for(int i=0; i<numServers; i++) {
      servers[i] = cluster.startServerVM(i, properties, locator0.getPort());
    }

    for(int i=0; i<numClients; i++) {
      clients[i] = getANewJedis();
    }

    AtomicBoolean done = new AtomicBoolean(false);
    int clientIndex = 0;

    // kick off some client threads(?) doing ops on servers and checking results
    while(!done.get()) {
      try {
        clients[clientIndex].set("key", "value");
    //   do Jedis operation
      } catch (JedisConnectionException e) {
        //   loop to get a new Jedis (maybe bail after numServers tries)
        clients[clientIndex] = getANewJedis();
        // confirm we can do a Jedis op...
        // if not, try to get a new Jedis again
        continue;
      }
     }
    // confirm data is there (maybe check all keys?)
    // }


    // start killing servers at some interval (20 seconds to start)


    for(int i=0; i<numStops; i++) {
      // choose a random server to kill
      int random = new Random().nextInt(numServers);
      // kill randomly selected server
      cluster.stop(random);
      // wait until the client has fully restarted
      for(int j=0; j<numServers; j++) {
        clients[i].waitTillClientsAreReadyOnServers(
            servers[j].getName(), servers[j].getPort(), numClients);
      }
    }
  }

  private Jedis getANewJedis() {
    return new Jedis(LOCAL_HOST, getNextServerPort(), JEDIS_TIMEOUT);
  }

  private int getNextServerPort() {
    return servers[(serverPortCounter++)%numServers].getPort();
  }
}
