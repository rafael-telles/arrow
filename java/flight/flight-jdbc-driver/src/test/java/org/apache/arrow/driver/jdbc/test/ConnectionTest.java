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

import static org.junit.Assert.*;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.arrow.driver.jdbc.ArrowFlightConnection;
import org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver;
import org.apache.arrow.driver.jdbc.client.ArrowFlightClientHandler;
import org.apache.arrow.driver.jdbc.test.utils.FlightTestUtils;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.GeneratedBearerTokenAuthenticator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Strings;

import io.grpc.Metadata;

/**
 * Tests for {@link Connection}.
 */
public class ConnectionTest {

  private FlightServer server;
  private String serverUrl;
  private BufferAllocator allocator;
  private FlightTestUtils flightTestUtils;

  /**
   * Setup for all tests.
   *
   * @throws ClassNotFoundException
   *           If the {@link ArrowFlightJdbcDriver} cannot be loaded.
   */
  @Before
  public void setUp() throws Exception {
    allocator = new RootAllocator(Long.MAX_VALUE);

    flightTestUtils = new FlightTestUtils("localhost", "flight1", "woho1",
        "invalid", "wrong");

    final FlightProducer flightProducer = flightTestUtils
        .getFlightProducer(allocator);
    this.server = flightTestUtils.getStartedServer(
        location -> FlightServer.builder(allocator, location, flightProducer)
            .headerAuthenticator(new GeneratedBearerTokenAuthenticator(
                new BasicCallHeaderAuthenticator(this::validate)))
            .build());
    serverUrl = flightTestUtils.getConnectionPrefix() +
        flightTestUtils.getLocalhost() + ":" + this.server.getPort();

    // TODO Double-check this later.
    Class.forName("org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver");
  }

  @After
  public void tearDown() throws Exception {
    AutoCloseables.close(server, allocator);
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
  private CallHeaderAuthenticator.AuthResult validate(final String username,
      final String password) {
    if (Strings.isNullOrEmpty(username)) {
      throw CallStatus.UNAUTHENTICATED
          .withDescription("Credentials not supplied.").toRuntimeException();
    }
    final String identity;
    if (flightTestUtils.getUsername1().equals(username) &&
        flightTestUtils.getPassword1().equals(password)) {
      identity = flightTestUtils.getUsername1();
    } else {
      throw CallStatus.UNAUTHENTICATED
          .withDescription("Username or password is invalid.")
          .toRuntimeException();
    }
    return () -> identity;
  }

  /**
   * Checks if an unencrypted connection can be established successfully when
   * the provided valid credentials.
   *
   * @throws SQLException
   *           on error.
   */
  @Test
  public void testUnencryptedConnectionShouldOpenSuccessfullyWhenProvidedValidCredentials()
      throws Exception {
    final Properties properties = new Properties();

    properties.put("user", flightTestUtils.getUsername1());
    properties.put("password", flightTestUtils.getPassword1());

    try (Connection connection = DriverManager.getConnection(serverUrl,
        properties)) {
      assert connection.isValid(300);
    }
  }

  /**
   * Checks if the exception SQLException is thrown when trying to establish a connection without a host.
   *
   * @throws SQLException
   *           on error.
   */
  @Test(expected = SQLException.class)
  public void testUnencryptedConnectionWithEmptyHost()
      throws Exception {
    final Properties properties = new Properties();

    properties.put("user", flightTestUtils.getUsername1());
    properties.put("password", flightTestUtils.getPassword1());
    String invalidUrl = flightTestUtils.getConnectionPrefix();

    try (Connection connection = DriverManager
        .getConnection(invalidUrl, properties)) {
      Assert.fail();
    }
  }

  /**
   * Try to instantiate a basic FlightClient.
   *
   * @throws URISyntaxException
   *           on error.
   */
  @Test
  public void testGetBasicClientAuthenticatedShouldOpenConnection()
      throws Exception {

    try (ArrowFlightClientHandler client = ArrowFlightClientHandler.getClient(
        allocator, flightTestUtils.getLocalhost(), this.server.getPort(),
        flightTestUtils.getUsername1(), flightTestUtils.getPassword1())) {
      assertNotNull(client);
    }
  }

  /**
   * Checks if the exception IllegalArgumentException is thrown when trying to establish an  unencrypted
   * connection providing with an invalid port.
   *
   * @throws SQLException
   *           on error.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testUnencryptedConnectionProvidingInvalidPort()
      throws Exception {
    final Properties properties = new Properties();

    properties.put("user", flightTestUtils.getUsername1());
    properties.put("password", flightTestUtils.getPassword1());
    String invalidUrl = flightTestUtils.getConnectionPrefix() + flightTestUtils.getLocalhost() + ":" + 65537;

    try (Connection connection = DriverManager
        .getConnection(invalidUrl, properties)) {
      Assert.fail();
    }
  }

  @Test(expected = SQLException.class)
  public void testReloadClientShouldThrowException()
      throws Exception {
    try (Connection connection = DriverManager.getConnection(serverUrl, new Properties())) {
      Method loadClient = ((ArrowFlightConnection) connection)
          .getClass().getDeclaredMethod("loadClient");
      loadClient.setAccessible(true);
      try {
        loadClient.invoke(connection);
      } catch (InvocationTargetException e) {
        Throwable throwable = e.getCause();
        if (throwable instanceof SQLException) {
          throw (SQLException) throwable;
        }
      }
    }
  }

  @Test
  public void testGetHeadersShouldReturnPropertiesAsHeaders()
      throws Exception {

    Properties properties = new Properties();
    properties.put("TEST", "PROPERTY");
    properties.put("ONCE", "MORE");
    properties.put("SHOULD", "SAVE");

    try (Connection connection = DriverManager.getConnection(serverUrl, properties)) {
      Method getHeaders = ((ArrowFlightConnection) connection)
          .getClass().getDeclaredMethod("getHeaders");
      getHeaders.setAccessible(true);

      HeaderCallOption headers =
          (HeaderCallOption) getHeaders.invoke(connection);

      Field propertiesMetadata =
          headers.getClass().getDeclaredField("propertiesMetadata");
      propertiesMetadata.setAccessible(true);
      Metadata metadata = (Metadata) propertiesMetadata.get(headers);

      assertEquals(
          metadata.get(Metadata.Key.of("TEST", Metadata.ASCII_STRING_MARSHALLER)),
          "PROPERTY"
      );
      assertEquals(
          metadata.get(Metadata.Key.of("ONCE", Metadata.ASCII_STRING_MARSHALLER)),
          "MORE"
      );
      assertEquals(
          metadata.get(Metadata.Key.of("SHOULD", Metadata.ASCII_STRING_MARSHALLER)),
          "SAVE"
      );
    }
  }

  /**
   * Try to instantiate a basic FlightClient.
   *
   * @throws URISyntaxException
   *           on error.
   */
  @Test
  public void testGetBasicClientNoAuthShouldOpenConnection() throws Exception {

    try (ArrowFlightClientHandler client = ArrowFlightClientHandler.getClient(
        allocator, flightTestUtils.getLocalhost(), this.server.getPort())) {
      assertNotNull(client);
    }
  }

  /**
   * Checks if an unencrypted connection can be established successfully when
   * not providing credentials.
   *
   * @throws SQLException
   *           on error.
   */
  @Test
  public void testUnencryptedConnectionShouldOpenSuccessfullyWithoutAuthentication()
      throws Exception {
    final Properties properties = new Properties();

    try (Connection connection = DriverManager
        .getConnection(serverUrl, properties)) {
      assert connection.isValid(300);
    }
  }

  /**
   * Check if an unencrypted connection throws an exception when provided with
   * invalid credentials.
   *
   * @throws SQLException
   *           The exception expected to be thrown.
   */
  @Test(expected = SQLException.class)
  public void testUnencryptedConnectionShouldThrowExceptionWhenProvidedWithInvalidCredentials()
      throws Exception {

    final Properties properties = new Properties();

    properties.put("user", flightTestUtils.getUsernameInvalid());
    properties.put("password", flightTestUtils.getPasswordInvalid());

    try (Connection connection = DriverManager.getConnection(serverUrl,
        properties)) {
      Assert.fail();
    }
  }
}
