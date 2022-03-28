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

package org.apache.arrow.driver.jdbc.utils;

import java.sql.SQLException;
import java.util.Map;

import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;

import com.google.common.collect.ImmutableMap;

/**
 * Parent class for rpc exceptions.
 */
public class FlightToJDBCExceptionMapper {

  private static final String SQL_STATE_UNAUTHENTICATED = "28000";
  private static final String SQL_STATE_UNAUTHORIZED = "42000";
  private static final String SQL_STATE_UNAVAILABLE = "08001";
  private static final String SQL_STATE_UNIMPLEMENTED = "0A000";
  private static final String SQL_STATE_CANCELLED = "HY008";
  private static final String SQL_STATE_ALREADY_EXISTS = "21000";
  private static final String SQL_STATE_NOT_FOUND = "42000";
  private static final String SQL_STATE_TIMED_OUT = "HYT01";
  private static final String SQL_STATE_INVALID_ARGUMENT = "2200T";
  private static final String SQL_STATE_INTERNAL = "01000";
  private static final String SQL_STATE_UNKNOWN = "01000";

  private static final Map<FlightStatusCode, String> errorStatusSQLStateMap =
      new ImmutableMap.Builder<FlightStatusCode, String>()
          .put(FlightStatusCode.UNAUTHENTICATED, SQL_STATE_UNAUTHENTICATED)
          .put(FlightStatusCode.UNAUTHORIZED, SQL_STATE_UNAUTHORIZED)
          .put(FlightStatusCode.UNAVAILABLE, SQL_STATE_UNAVAILABLE)
          .put(FlightStatusCode.UNIMPLEMENTED, SQL_STATE_UNIMPLEMENTED)
          .put(FlightStatusCode.CANCELLED, SQL_STATE_CANCELLED)
          .put(FlightStatusCode.ALREADY_EXISTS, SQL_STATE_ALREADY_EXISTS)
          .put(FlightStatusCode.NOT_FOUND, SQL_STATE_NOT_FOUND)
          .put(FlightStatusCode.TIMED_OUT, SQL_STATE_TIMED_OUT)
          .put(FlightStatusCode.INVALID_ARGUMENT, SQL_STATE_INVALID_ARGUMENT)
          .put(FlightStatusCode.INTERNAL, SQL_STATE_INTERNAL)
          .put(FlightStatusCode.UNKNOWN, SQL_STATE_UNKNOWN).build();

  private FlightToJDBCExceptionMapper() {}

  public static SQLException map(FlightRuntimeException flightRuntimeException) {
    return map(flightRuntimeException, flightRuntimeException.getMessage());
  }

  /**
   * Map the given RpcException into an equivalent SQLException.
   * <p>
   * An appropriate SQLState will be chosen for the RpcException, if one is available.
   *
   * @param flightRuntimeException The remote exception to map.
   * @param message                The message format string to use for the SQLException.
   * @return The equivalently mapped SQLException.
   */
  public static SQLException map(FlightRuntimeException flightRuntimeException, String message) {
    return new SQLException(message, getSqlCodeFromRpcExceptionType(flightRuntimeException), flightRuntimeException);
  }

  private static String getSqlCodeFromRpcExceptionType(FlightRuntimeException flightRuntimeException) {
    return errorStatusSQLStateMap.get(flightRuntimeException.status().code());
  }
}
