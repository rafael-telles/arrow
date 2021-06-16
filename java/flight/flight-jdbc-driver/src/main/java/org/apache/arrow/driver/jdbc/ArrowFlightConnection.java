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

import static org.apache.arrow.driver.jdbc.utils.BaseProperty.HOST;
import static org.apache.arrow.driver.jdbc.utils.BaseProperty.KEYSTORE_PASS;
import static org.apache.arrow.driver.jdbc.utils.BaseProperty.KEYSTORE_PATH;
import static org.apache.arrow.driver.jdbc.utils.BaseProperty.PASSWORD;
import static org.apache.arrow.driver.jdbc.utils.BaseProperty.PORT;
import static org.apache.arrow.driver.jdbc.utils.BaseProperty.USERNAME;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.arrow.driver.jdbc.client.ArrowFlightClientHandler;
import org.apache.arrow.driver.jdbc.utils.BaseProperty;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.Preconditions;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaFactory;

import com.google.common.base.Strings;

/**
 * Connection to the Arrow Flight server.
 */
public class ArrowFlightConnection extends AvaticaConnection {

  private final BufferAllocator allocator;

  // TODO Use this later to run queries.
  @SuppressWarnings("unused")
  private ArrowFlightClientHandler client;

  /**
   * Instantiates a new Arrow Flight Connection.
   *
   * @param driver
   *          The JDBC driver to use.
   * @param factory
   *          The Avatica Factory to use.
   * @param url
   *          The URL to connect to.
   * @param info
   *          The properties of this connection.
   * @throws SQLException
   *           If the connection cannot be established.
   */
  protected ArrowFlightConnection(final ArrowFlightJdbcDriver driver,
      final AvaticaFactory factory, final String url, final Properties info)
      throws SQLException {
    super(driver, factory, url, info);
    allocator = new RootAllocator(Integer.MAX_VALUE);

    try {
      loadClient();
    } catch (final SQLException e) {
      allocator.close();
      throw new SQLException("Failed to initialize Flight Client.", e);
    }
  }

  /**
   * Sets {@link #client} based on the properties of this connection.
   *
   * @throws KeyStoreException
   *           If an error occurs while trying to retrieve KeyStore information.
   * @throws NoSuchAlgorithmException
   *           If a particular cryptographic algorithm is required but does not
   *           exist.
   * @throws CertificateException
   *           If an error occurs while trying to retrieve certificate
   *           information.
   * @throws IOException
   *           If an I/O operation fails.
   * @throws NumberFormatException
   *           If the port number to connect to is invalid.
   * @throws URISyntaxException
   *           If the URI syntax is invalid.
   */
  private void loadClient() throws SQLException {

    if (client != null) {
      throw new SQLException("Client already loaded.",
          new IllegalStateException());
    }

    // =================== [ LOCATION CONFIG ] ===================
    final Map.Entry<Object, Object> forHost = HOST.getEntry();

    final String host = (String) info.getOrDefault(forHost.getKey(),
        forHost.getValue());
    Preconditions.checkArgument(!Strings.isNullOrEmpty(host));

    final Map.Entry<Object, Object> forPort = PORT.getEntry();

    final int port = Integer.parseInt(Objects
            .toString(info.getOrDefault(forPort.getKey(), forPort.getValue())));
    Preconditions.checkArgument(0 < port && port < 65536,
        "Port number must be between exclusive range (0, 65536).");

    // =================== [ CREDENTIALS CONFIG ] ===================
    final Map.Entry<Object, Object> forUsername = USERNAME.getEntry();

    final String username = (String) info.getOrDefault(forUsername.getKey(),
        forUsername.getValue());

    final Map.Entry<Object, Object> forPassword = PASSWORD.getEntry();

    final String password = (String) info.getOrDefault(forPassword.getKey(),
        forPassword.getValue());

    // =================== [ ENCRYPTION CONFIG ] ===================
    final Map.Entry<Object, Object> forKeyStorePath = KEYSTORE_PATH.getEntry();

    final String keyStorePath = (String) info
        .getOrDefault(forKeyStorePath.getKey(), forKeyStorePath.getValue());

    final Map.Entry<Object, Object> forKeyStorePass = KEYSTORE_PASS.getEntry();

    final String keyStorePassword = (String) info
        .getOrDefault(forKeyStorePass.getKey(), forKeyStorePass.getValue());

    // =================== [ CLIENT GENERATION ] ===================
    try {
      client = ArrowFlightClientHandler.getClient(allocator, host, port,
          username, password, getHeaders(), keyStorePath, keyStorePassword);
    } catch (GeneralSecurityException | IOException e) {
      throw new SQLException("Failed to connect to the Arrow Flight client.",
          e);
    }
  }

  private HeaderCallOption getHeaders() {

    final CallHeaders headers = new FlightCallHeaders();
    final Iterator<Map.Entry<Object, Object>> properties = info.entrySet()
        .iterator();
    final Set<Object> connectionProperties =
        new HashSet<Object>(Arrays.stream(BaseProperty.values())
            .map(baseProperty -> baseProperty.getEntry().getKey())
            .collect(Collectors.toUnmodifiableList()));

    while (properties.hasNext()) {
      final Map.Entry<Object, Object> entry = properties.next();
      final Object key = entry.getKey();

      /*
       * If the current property if not a BaseProperty, it must be a
       * custom parameter that can be passed to the client as a header
       * for subsequent calls.
       */
      if (!connectionProperties.contains(key)) {
        headers.insert(Objects.toString(entry.getKey()),
            Objects.toString(entry.getValue()));
      }
    }

    return new HeaderCallOption(headers);
  }

  @Override
  public void close() throws SQLException {

    try {
      AutoCloseables.close(client, allocator);
    } catch (final Exception e) {
      throw new SQLException("Failed to close resources.", e);
    }

    super.close();
  }

}
