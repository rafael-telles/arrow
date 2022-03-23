/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.driver.jdbc.client.utils;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeyedFlightSqlClientObjectPoolFactory extends BaseKeyedPooledObjectFactory<Location, FlightSqlClient> {
  private static final Logger logger = LoggerFactory.getLogger(KeyedFlightSqlClientObjectPoolFactory.class);
  private final BufferAllocator parentAllocator;
  private final AtomicInteger clientCounter = new AtomicInteger();

  public KeyedFlightSqlClientObjectPoolFactory(final BufferAllocator parentAllocator) {
    super();
    this.parentAllocator = parentAllocator
        .newChildAllocator("KeyedFlightSqlClientObjectPoolFactory", 0, parentAllocator.getLimit());
  }

  @Override
  public FlightSqlClient create(Location key) throws Exception {
    logger.info("Trying to create a new FlightSqlClient.");
    return new FlightSqlClient(
        FlightClient.builder(
            parentAllocator.newChildAllocator(
                "flight-sql-client-pool_id-" + clientCounter.getAndIncrement(),
                0,
                parentAllocator.getLimit()),
            key).build());
  }

  public void closeAllocator() {
    parentAllocator.getChildAllocators().forEach(BufferAllocator::close);
    parentAllocator.close();
  }

  @Override
  public PooledObject<FlightSqlClient> wrap(FlightSqlClient value) {
    logger.info("Wrapping a existing FlightSqlClient.");
    return new DefaultPooledObject<>(value);
  }

  @Override
  public void destroyObject(Location key, PooledObject<FlightSqlClient> p) throws Exception {
    logger.info("Closing a client.");
    p.getObject().close();
  }
}
