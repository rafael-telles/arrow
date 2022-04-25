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

package org.apache.arrow.driver.jdbc;

import static org.hamcrest.CoreMatchers.equalTo;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.arrow.driver.jdbc.utils.CoreMockedSqlProducers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

public class ArrowFlightPreparedStatementTest {

  @ClassRule
  public static final FlightServerTestRule FLIGHT_SERVER_TEST_RULE = FlightServerTestRule
      .createStandardTestRule(CoreMockedSqlProducers.getLegacyProducer());

  private static Connection connection;

  @Rule
  public final ErrorCollector collector = new ErrorCollector();

  @BeforeClass
  public static void setup() throws SQLException {
    connection = FLIGHT_SERVER_TEST_RULE.getConnection(false);
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    connection.close();
  }

  @Test
  public void testSimpleQueryNoParameterBinding() throws SQLException {
    TimeZone timeZone = TimeZone.getTimeZone("Asia/Tokyo");
    Calendar calendar = Calendar.getInstance(timeZone);

    Properties properties = new Properties();
    properties.put("user", "dremio");
    properties.put("password", "dremio123");

    final String query = "SELECT DATE '1401-06-01' as literal";
    try (
        Connection connection1 = DriverManager
            .getConnection("jdbc:arrow-flight://automaster.drem.io:32010?useEncryption=false", properties);
        final PreparedStatement preparedStatement = connection1.prepareStatement(query);
        final ResultSet resultSet = preparedStatement.executeQuery()) {

      while (resultSet.next()) {
        Date date = resultSet.getDate(1, calendar);
        Date date2 = resultSet.getDate(1);
        System.out.println(1);
      }
    }
  }

  @Test
  public void testReturnColumnCount() throws SQLException {
    final String query = CoreMockedSqlProducers.LEGACY_REGULAR_SQL_CMD;
    try (final PreparedStatement psmt = connection.prepareStatement(query)) {
      collector.checkThat("ID", equalTo(psmt.getMetaData().getColumnName(1)));
      collector.checkThat("Name", equalTo(psmt.getMetaData().getColumnName(2)));
      collector.checkThat("Age", equalTo(psmt.getMetaData().getColumnName(3)));
      collector.checkThat("Salary", equalTo(psmt.getMetaData().getColumnName(4)));
      collector.checkThat("Hire Date", equalTo(psmt.getMetaData().getColumnName(5)));
      collector.checkThat("Last Sale", equalTo(psmt.getMetaData().getColumnName(6)));
      collector.checkThat(6, equalTo(psmt.getMetaData().getColumnCount()));
    }
  }
}
