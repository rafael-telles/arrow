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

import static org.junit.Assert.assertNotNull;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Properties;

import org.apache.arrow.driver.jdbc.adhoc.MockFlightSqlProducer;
import org.apache.arrow.driver.jdbc.authentication.UserPasswordAuthentication;
import org.apache.arrow.driver.jdbc.client.ArrowFlightSqlClientHandler;
import org.apache.arrow.driver.jdbc.utils.ArrowFlightConnectionConfigImpl.ArrowFlightConnectionProperty;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.calcite.avatica.BuiltInConnectionProperty;
import org.apache.calcite.avatica.org.apache.http.auth.UsernamePasswordCredentials;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import com.google.common.base.Strings;

/**
 * Tests encrypted connections.
 */
public class ConnectionTlsTest {
  private static final MockFlightSqlProducer PRODUCER = new MockFlightSqlProducer();

  @ClassRule
  public static FlightServerTestRule FLIGHT_SERVER_TEST_RULE = null;
  private final String keyStorePath = this.getClass().getResource("/keys/keyStore.jks")
      .getPath();
  private final String noCertificateKeyStorePath =
      this.getClass().getResource("/keys/noCertificate.jks")
          .getPath();
  private final String keyStorePass = "flight";
  private BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
  private ArrowFlightJdbcConnectionPoolDataSource dataSource;

  @Before
  public void setUp() throws Exception {
    final FlightServerTestRule.CertKeyPair certKey = FlightServerTestRule
        .exampleTlsCerts().get(0);

    UserPasswordAuthentication authentication =
        new UserPasswordAuthentication.Builder().user("user1", "pass1").user("user2", "pass2")
            .build();

    FLIGHT_SERVER_TEST_RULE = new FlightServerTestRule.Builder()
        .host("localhost")
        .randomPort()
        .authentication(authentication)
        .withTlsEncryption(true)
        .producer(PRODUCER).build();

    dataSource = FLIGHT_SERVER_TEST_RULE.createConnectionPoolDataSource();
  }

  @After
  public void tearDown() throws Exception {
    Collection<BufferAllocator> childAllocators = allocator.getChildAllocators();
    AutoCloseables.close(childAllocators.toArray(new AutoCloseable[0]));
    AutoCloseables.close(dataSource, allocator);
  }

  /**
   * Validate the user's credential on a FlightServer.
   *
   * @param username flight server username.
   * @param password flight server password.
   * @return the result of validation.
   */
  private CallHeaderAuthenticator.AuthResult validate(final String username,
                                                      final String password) {
    if (Strings.isNullOrEmpty(username)) {
      throw CallStatus.UNAUTHENTICATED
          .withDescription("Credentials not supplied.").toRuntimeException();
    }
    final String identity;
    if (dataSource.getConfig().getUser().equals(username) &&
        dataSource.getConfig().getPassword().equals(password)) {
      identity = dataSource.getConfig().getUser();
    } else {
      throw CallStatus.UNAUTHENTICATED
          .withDescription("Username or password is invalid.")
          .toRuntimeException();
    }
    return () -> identity;
  }

  /**
   * Try to instantiate an encrypted FlightClient.
   *
   * @throws Exception on error.
   */
  @Test
  public void testGetEncryptedClientAuthenticated() throws Exception {
    final UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(
        dataSource.getConfig().getUser(), dataSource.getConfig().getPassword());

    try (ArrowFlightSqlClientHandler client =
             new ArrowFlightSqlClientHandler.Builder()
                 .withHost(dataSource.getConfig().getHost())
                 .withPort(dataSource.getConfig().getPort())
                 .withUsername(credentials.getUserName())
                 .withPassword(credentials.getPassword())
                 .withKeyStorePath(keyStorePath)
                 .withKeyStorePassword(keyStorePass)
                 .withBufferAllocator(allocator)
                 .withTlsEncryption(true)
                 .build()) {
      assertNotNull(client);
    }
  }

  /**
   * Try to instantiate an encrypted FlightClient providing a keystore without certificate. It's expected to
   * receive the SQLException.
   *
   * @throws Exception on error.
   */
  @Test(expected = SQLException.class)
  public void testGetEncryptedClientWithNoCertificateOnKeyStore() throws Exception {
    final String noCertificateKeyStorePassword = "flight1";

    try (ArrowFlightSqlClientHandler client =
             new ArrowFlightSqlClientHandler.Builder()
                 .withHost(dataSource.getConfig().getHost())
                 .withKeyStorePath(noCertificateKeyStorePath)
                 .withKeyStorePassword(noCertificateKeyStorePassword)
                 .withBufferAllocator(allocator)
                 .withTlsEncryption(true)
                 .build()) {
      Assert.fail();
    }
  }

  /**
   * Try to instantiate an encrypted FlightClient without credentials.
   *
   * @throws Exception on error.
   */
  @Test
  public void testGetNonAuthenticatedEncryptedClientNoAuth() throws Exception {
    try (ArrowFlightSqlClientHandler client =
             new ArrowFlightSqlClientHandler.Builder()
                 .withHost(dataSource.getConfig().getHost())
                 .withKeyStorePath(keyStorePath)
                 .withKeyStorePassword(keyStorePass)
                 .withBufferAllocator(allocator)
                 .withTlsEncryption(true)
                 .build()) {
      assertNotNull(client);
    }
  }

  /**
   * Try to instantiate an encrypted FlightClient with an invalid password to the keystore file.
   * It's expected to receive the SQLException.
   *
   * @throws Exception on error.
   */
  @Test(expected = SQLException.class)
  public void testGetEncryptedClientWithKeyStoreBadPasswordAndNoAuth() throws Exception {
    String keyStoreBadPassword = "badPassword";

    try (ArrowFlightSqlClientHandler client =
             new ArrowFlightSqlClientHandler.Builder()
                 .withHost(dataSource.getConfig().getHost())
                 .withKeyStorePath(keyStorePath)
                 .withKeyStorePassword(keyStoreBadPassword)
                 .withBufferAllocator(allocator)
                 .withTlsEncryption(true)
                 .build()) {
      Assert.fail();
    }
  }

  /**
   * Check if an encrypted connection can be established successfully when the
   * provided valid credentials and a valid Keystore.
   *
   * @throws Exception on error.
   */
  @Test
  public void testGetEncryptedConnectionWithValidCredentialsAndKeyStore() throws Exception {
    final Properties properties = new Properties();

    properties.put(ArrowFlightConnectionProperty.HOST.camelName(), "localhost");
    properties.put(ArrowFlightConnectionProperty.PORT.camelName(),
        dataSource.getConfig().getPort());
    properties.put(ArrowFlightConnectionProperty.USER.camelName(),
        dataSource.getConfig().getUser());
    properties.put(ArrowFlightConnectionProperty.PASSWORD.camelName(),
        dataSource.getConfig().getPassword());
    properties.put(ArrowFlightConnectionProperty.USE_TLS.camelName(), true);
    properties.put(BuiltInConnectionProperty.KEYSTORE.camelName(), keyStorePath);
    properties.put(BuiltInConnectionProperty.KEYSTORE_PASSWORD.camelName(), keyStorePass);

    final ArrowFlightJdbcDataSource dataSource =
        ArrowFlightJdbcDataSource.createNewDataSource(properties);
    try (final Connection connection = dataSource.getConnection()) {
      assert connection.isValid(300);
    }
  }

  /**
   * Check if the SQLException is thrown when trying to establish an encrypted connection
   * providing valid credentials but invalid password to the Keystore.
   *
   * @throws SQLException on error.
   */
  @Test(expected = SQLException.class)
  public void testGetAuthenticatedEncryptedConnectionWithKeyStoreBadPassword() throws Exception {
    final Properties properties = new Properties();

    properties.put(ArrowFlightConnectionProperty.HOST.camelName(),
        dataSource.getConfig().getHost());
    properties.put(ArrowFlightConnectionProperty.PORT.camelName(),
        dataSource.getConfig().getPort());
    properties.put(ArrowFlightConnectionProperty.USER.camelName(),
        dataSource.getConfig().getUser());
    properties.put(ArrowFlightConnectionProperty.PASSWORD.camelName(),
        dataSource.getConfig().getPassword());
    properties.put(ArrowFlightConnectionProperty.USE_TLS.camelName(), true);
    properties.put(BuiltInConnectionProperty.KEYSTORE.camelName(), keyStorePath);
    properties.put(BuiltInConnectionProperty.KEYSTORE_PASSWORD.camelName(), "badpassword");

    final ArrowFlightJdbcDataSource dataSource =
        ArrowFlightJdbcDataSource.createNewDataSource(properties);
    try (final Connection connection = dataSource.getConnection()) {
      Assert.fail();
    }
  }

  /**
   * Check if an encrypted connection can be established successfully when not providing authentication.
   *
   * @throws Exception on error.
   */
  @Test
  public void testGetNonAuthenticatedEncryptedConnection() throws Exception {
    final Properties properties = new Properties();

    properties.put(ArrowFlightConnectionProperty.HOST.camelName(),
        dataSource.getConfig().getHost());
    properties.put(ArrowFlightConnectionProperty.PORT.camelName(),
        dataSource.getConfig().getPort());
    properties.put(ArrowFlightConnectionProperty.USE_TLS.camelName(), true);
    properties.put(BuiltInConnectionProperty.KEYSTORE.camelName(), keyStorePath);
    properties.put(BuiltInConnectionProperty.KEYSTORE_PASSWORD.camelName(), keyStorePass);

    final ArrowFlightJdbcDataSource dataSource =
        ArrowFlightJdbcDataSource.createNewDataSource(properties);
    try (final Connection connection = dataSource.getConnection()) {
      assert connection.isValid(300);
    }
  }

  /**
   * Check if an encrypted connection can be established successfully when connecting through the DriverManager using
   * just a connection url.
   *
   * @throws Exception on error.
   */
  @Test
  public void testTLSConnectionPropertyTrueCorrectCastUrlWithDriverManager() throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();
    DriverManager.registerDriver(driver);

    Assert.assertTrue(DriverManager.getConnection(
        String.format(
            "jdbc:arrow-flight://localhost:%s?user=%s&password=%s&useTls=true&%s=%s&%s=%s",
            dataSource.getConfig().getPort(),
            dataSource.getConfig().getUser(),
            dataSource.getConfig().getPassword(),
            BuiltInConnectionProperty.KEYSTORE.camelName(),
            keyStorePath,
            BuiltInConnectionProperty.KEYSTORE_PASSWORD.camelName(),
            keyStorePass)).isValid(0));
  }

  /**
   * Check if an encrypted connection can be established successfully when connecting through the DriverManager using
   * a connection url and properties with String K-V pairs.
   *
   * @throws Exception on error.
   */
  @Test
  public void testTLSConnectionPropertyTrueCorrectCastUrlAndPropertiesUsingSetPropertyWithDriverManager()
      throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();
    DriverManager.registerDriver(driver);

    Properties properties = new Properties();

    properties.setProperty(ArrowFlightConnectionProperty.USER.camelName(),
        dataSource.getConfig().getUser());
    properties.setProperty(ArrowFlightConnectionProperty.PASSWORD.camelName(),
        dataSource.getConfig().getPassword());
    properties.setProperty(BuiltInConnectionProperty.KEYSTORE.camelName(), keyStorePath);
    properties.setProperty(BuiltInConnectionProperty.KEYSTORE_PASSWORD.camelName(), keyStorePass);
    properties.setProperty(ArrowFlightConnectionProperty.USE_TLS.camelName(), "true");
    Assert.assertTrue(DriverManager.getConnection(
        String.format(
            "jdbc:arrow-flight://localhost:%s",
            dataSource.getConfig().getPort()),
        properties).isValid(0));
  }

  /**
   * Check if an encrypted connection can be established successfully when connecting through the DriverManager using
   * a connection url and properties with Object K-V pairs.
   *
   * @throws Exception on error.
   */
  @Test
  public void testTLSConnectionPropertyTrueCorrectCastUrlAndPropertiesUsingPutWithDriverManager()
      throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();
    DriverManager.registerDriver(driver);

    Properties properties = new Properties();

    properties.put(ArrowFlightConnectionProperty.USER.camelName(),
        dataSource.getConfig().getUser());
    properties.put(ArrowFlightConnectionProperty.PASSWORD.camelName(),
        dataSource.getConfig().getPassword());
    properties.put(ArrowFlightConnectionProperty.USE_TLS.camelName(), true);
    properties.put(BuiltInConnectionProperty.KEYSTORE.camelName(), keyStorePath);
    properties.put(BuiltInConnectionProperty.KEYSTORE_PASSWORD.camelName(), keyStorePass);

    Assert.assertTrue(DriverManager.getConnection(
        String.format(
            "jdbc:arrow-flight://localhost:%s",
            dataSource.getConfig().getPort()),
        properties).isValid(0));
  }

  /**
   * Check if an encrypted connection can be established successfully when connecting through the DriverManager using
   * just a connection url and using 0 and 1 as useTls values.
   *
   * @throws Exception on error.
   */
  @Test
  public void testTLSConnectionPropertyTrueIntegerCorrectCastUrlWithDriverManager()
      throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();
    DriverManager.registerDriver(driver);

    Assert.assertTrue(DriverManager.getConnection(
        String.format(
            "jdbc:arrow-flight://localhost:%s?user=%s&password=%s&useTls=1&%s=%s&%s=%s",
            dataSource.getConfig().getPort(),
            dataSource.getConfig().getUser(),
            dataSource.getConfig().getPassword(),
            BuiltInConnectionProperty.KEYSTORE.camelName(),
            keyStorePath,
            BuiltInConnectionProperty.KEYSTORE_PASSWORD.camelName(),
            keyStorePass)).isValid(0));
  }

  /**
   * Check if an encrypted connection can be established successfully when connecting through the DriverManager using
   * a connection url and properties with String K-V pairs and using 0 and 1 as useTls values.
   *
   * @throws Exception on error.
   */
  @Test
  public void testTLSConnectionPropertyTrueIntegerCorrectCastUrlAndPropertiesUsingSetPropertyWithDriverManager()
      throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();
    DriverManager.registerDriver(driver);

    Properties properties = new Properties();

    properties.setProperty(ArrowFlightConnectionProperty.USER.camelName(),
        dataSource.getConfig().getUser());
    properties.setProperty(ArrowFlightConnectionProperty.PASSWORD.camelName(),
        dataSource.getConfig().getPassword());
    properties.setProperty(BuiltInConnectionProperty.KEYSTORE.camelName(), keyStorePath);
    properties.setProperty(BuiltInConnectionProperty.KEYSTORE_PASSWORD.camelName(), keyStorePass);
    properties.setProperty(ArrowFlightConnectionProperty.USE_TLS.camelName(), "1");

    Assert.assertTrue(DriverManager.getConnection(
        String.format("jdbc:arrow-flight://localhost:%s", dataSource.getConfig().getPort()),
        properties).isValid(0));
  }

  /**
   * Check if an encrypted connection can be established successfully when connecting through the DriverManager using
   * a connection url and properties with Object K-V pairs and using 0 and 1 as useTls values.
   *
   * @throws Exception on error.
   */
  @Test
  public void testTLSConnectionPropertyTrueIntegerCorrectCastUrlAndPropertiesUsingPutWithDriverManager()
      throws Exception {
    final Driver driver = new ArrowFlightJdbcDriver();
    DriverManager.registerDriver(driver);

    Properties properties = new Properties();

    properties.put(ArrowFlightConnectionProperty.USER.camelName(),
        dataSource.getConfig().getUser());
    properties.put(ArrowFlightConnectionProperty.PASSWORD.camelName(),
        dataSource.getConfig().getPassword());
    properties.put(ArrowFlightConnectionProperty.USE_TLS.camelName(), 1);
    properties.put(BuiltInConnectionProperty.KEYSTORE.camelName(), keyStorePath);
    properties.put(BuiltInConnectionProperty.KEYSTORE_PASSWORD.camelName(), keyStorePass);

    Assert.assertTrue(DriverManager.getConnection(
        String.format("jdbc:arrow-flight://localhost:%s",
            dataSource.getConfig().getPort()),
        properties).isValid(0));
  }
}
