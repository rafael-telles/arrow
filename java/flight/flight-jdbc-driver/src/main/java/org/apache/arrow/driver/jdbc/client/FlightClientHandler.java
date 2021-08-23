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

package org.apache.arrow.driver.jdbc.client;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.util.AutoCloseables;

/**
 * A wrapper for a {@link FlightClient}.
 */
public interface FlightClientHandler extends AutoCloseable {

  /**
   * Gets the {@link FlightClient} managed by this handler.
   *
   * @return the client.
   */
  FlightClient getClient();

  /**
   * Gets the call options for subsequent calls to the client wrapped by this handler.
   *
   * @return the call options.
   */
  CallOption[] getOptions();

  /**
   * Makes an RPC "getStream" request based on the provided {@link FlightInfo}
   * object. Retrieves the result of the query previously prepared with "getInfo."
   *
   * @param query The query.
   * @return a {@code FlightStream} of results.
   */
  default List<FlightStream> getStreams(String query) {
    return getInfo(query).getEndpoints().stream()
            .map(FlightEndpoint::getTicket)
            .map(ticket -> getClient().getStream(ticket, getOptions()))
            .collect(Collectors.toList());
  }

  /**
   * Makes an RPC "getInfo" request based on the provided {@code query}
   * object.
   *
   * @param query The query.
   * @return a {@code FlightStream} of results.
   */
  FlightInfo getInfo(String query);

  @Override
  default void close() throws Exception {
    AutoCloseables.close(getClient());
  }
}
