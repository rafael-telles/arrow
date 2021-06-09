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

package org.apache.arrow.driver.jdbc.test;

import static org.junit.Assert.assertArrayEquals;

import java.lang.reflect.Method;
import java.net.URI;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver;
import org.apache.arrow.driver.jdbc.test.utils.FlightTestUtils;
import org.apache.arrow.driver.jdbc.test.utils.PropertiesSample;
import org.apache.arrow.driver.jdbc.test.utils.UrlSample;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.GeneratedBearerTokenAuthenticator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Strings;

/**
 * Tests for {@link ArrowFlightJdbcDriver}.
 */
public class ArrowFlightJdbcDriverTest {

  private BufferAllocator allocator;
  private FlightServer server;
  FlightTestUtils testUtils;

  @Before
  public void setUp() throws Exception {
    // TODO Replace this.
    Class.forName("org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver");

    allocator = new RootAllocator(Long.MAX_VALUE);

    UrlSample url = UrlSample.CONFORMING;

    Properties propertiesConforming = PropertiesSample.CONFORMING
        .getProperties();

    Properties propertiesUnsupported = PropertiesSample.UNSUPPORTED
        .getProperties();

    testUtils = new FlightTestUtils(url.getHost(),
        propertiesConforming.getProperty("user"),
        propertiesConforming.getProperty("password"),
        propertiesUnsupported.getProperty("user"),
        propertiesUnsupported.getProperty("password"));

    final FlightProducer flightProducer = testUtils
        .getFlightProducer(allocator);

    server = testUtils
        .getStartedServer(
            (location -> FlightServer
                .builder(allocator, location, flightProducer)
                .headerAuthenticator(new GeneratedBearerTokenAuthenticator(
                    new BasicCallHeaderAuthenticator(this::validate)))
                .build()));
  }

  @After
  public void tearDown() throws Exception {
    AutoCloseables.close(server, allocator);
  }

  /**
   * Tests whether the {@link ArrowFlightJdbcDriver} is registered in the
   * {@link DriverManager}.
   * 
   * @throws SQLException
   *           If an error occurs. (This is not supposed to happen.)
   */
  @Test
  public void testDriverIsRegisteredInDriverManager() throws Exception {
    assert DriverManager.getDriver(
        UrlSample.CONFORMING.getPrefix()) instanceof ArrowFlightJdbcDriver;
  }

  /**
   * Tests whether the {@link ArrowFlightJdbcDriver} fails when provided with an
   * unsupported URL prefix.
   * 
   * @throws SQLException
   *           If the test passes.
   */
  @Test(expected = SQLException.class)
  public void testShouldDeclineUrlWithUnsupportedPrefix() throws Exception {
    Driver driver = new ArrowFlightJdbcDriver();

    driver.connect(UrlSample.UNSUPPORTED.getPath(),
        PropertiesSample.UNSUPPORTED.getProperties()).close();
  }

  /**
   * Tests whether the {@link ArrowFlightJdbcDriver} can establish a successful
   * connection to the Arrow Flight client.
   * 
   * @throws Exception
   *           If the connection fails to be established.
   */
  @Test
  public void testShouldConnectWhenProvidedWithValidUrl() throws Exception {
    // Get the Arrow Flight JDBC driver by providing a URL with a valid prefix.
    Driver driver = new ArrowFlightJdbcDriver();

    URI uri = server.getLocation().getUri();

    try (Connection connection = driver.connect(
        "jdbc:arrow-flight://" + uri.getHost() + ":" + uri.getPort(),
        PropertiesSample.CONFORMING.getProperties())) {
      assert connection.isValid(300);
    }
  }

  /**
   * Tests whether an exception is thrown upon attempting to connect to a
   * malformed URI.
   *
   * @throws Exception If an error occurs.
   */
  @Test(expected = SQLException.class)
  public void testShouldThrowExceptionWhenAttemptingToConnectToMalformedUrl()
      throws Exception {
    Driver driver = new ArrowFlightJdbcDriver();
    String malformedUri = "yes:??/chainsaw.i=T333";
    driver.connect(malformedUri, PropertiesSample.UNSUPPORTED.getProperties());
  }

  /**
   * Tests whether an exception is thrown upon attempting to connect to a
   * malformed URI.
   *
   * @throws Exception If an error occurs.
   */
  @Test(expected = SQLException.class)
  public void testShouldThrowExceptionWhenAttemptingToConnectToUrlNoPrefix()
      throws Exception {
    Driver driver = new ArrowFlightJdbcDriver();
    String malformedUri = server.getLocation().getUri().toString();
    driver.connect(malformedUri, PropertiesSample.UNSUPPORTED.getProperties());
  }

  /**
   * Tests whether an exception is thrown upon attempting to connect to a
   * malformed URI.
   *
   * @throws Exception If an error occurs.
   */
  @Test(expected = SQLException.class)
  public void testShouldThrowExceptionWhenAttemptingToConnectToUrlNoPort()
      throws Exception {
    Driver driver = new ArrowFlightJdbcDriver();
    String malformedUri = "arrow-jdbc://" +
        server.getLocation().getUri().getHost();
    driver.connect(malformedUri, PropertiesSample.UNSUPPORTED.getProperties());
  }

  /**
   * Tests whether an exception is thrown upon attempting to connect to a
   * malformed URI.
   *
   * @throws Exception If an error occurs.
   */
  @Test(expected = SQLException.class)
  public void testShouldThrowExceptionWhenAttemptingToConnectToUrlNoHost()
      throws Exception {
    Driver driver = new ArrowFlightJdbcDriver();

    String malformedUri = "arrow-jdbc://" +
        ":" + server.getLocation().getUri().getPort();
    driver.connect(malformedUri, PropertiesSample.UNSUPPORTED.getProperties());
  }

  /**
   * Tests whether an exception is thrown upon attempting to connect to a
   * malformed URI.
   *
   * @throws Exception If an error occurs.
   */
  @Test
  public void testDriverUrlParsingMechanismShouldReturnTheDesiredArgsFromUrl()
      throws Exception {
    Driver driver = new ArrowFlightJdbcDriver();

    Method getUrlArgs = driver.getClass()
        .getDeclaredMethod("getUrlsArgs", String.class);

    getUrlArgs.setAccessible(true);

    String[] parsedArgs = (String[]) getUrlArgs
        .invoke(driver, "jdbc:arrow-flight://localhost:32010");    
    
    assertArrayEquals(parsedArgs,
        new String[] {"localhost", "32010"});
  }

  /**
   * Validate the user's credential on a FlightServer.
   *
   * @param username
   *          flight server username.
   * @param password
   *          flight server password.
   * @return the result of validation.
   */
  private CallHeaderAuthenticator.AuthResult validate(String username,
      String password) {
    if (Strings.isNullOrEmpty(username)) {
      throw CallStatus.UNAUTHENTICATED
          .withDescription("Credentials not supplied.").toRuntimeException();
    }
    final String identity;
    if (testUtils.getUsername1().equals(username) &&
          testUtils.getPassword1().equals(password)) {
      identity = testUtils.getUsername1();
    } else {
      throw CallStatus.UNAUTHENTICATED
          .withDescription("Username or password is invalid.")
          .toRuntimeException();
    }
    return () -> identity;
  }

}