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

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.arrow.driver.jdbc.client.utils.ClientAuthenticationUtils;
import org.apache.arrow.driver.jdbc.client.utils.KeyedFlightSqlClientObjectPool;
import org.apache.arrow.driver.jdbc.client.utils.KeyedFlightSqlClientObjectPoolFactory;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightClientMiddleware;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.auth2.BearerCredentialWriter;
import org.apache.arrow.flight.auth2.ClientBearerHeaderHandler;
import org.apache.arrow.flight.auth2.ClientIncomingAuthHeaderMiddleware;
import org.apache.arrow.flight.client.ClientCookieMiddleware;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlInfo;
import org.apache.arrow.flight.sql.util.TableRef;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.avatica.Meta.StatementType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link FlightSqlClient} handler.
 */
public final class ArrowFlightSqlClientHandler implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(ArrowFlightSqlClientHandler.class);
  private final FlightSqlClient sqlClient;
  private final Set<CallOption> options = new HashSet<>();
  private final KeyedFlightSqlClientObjectPool flightSqlClientPool;

  ArrowFlightSqlClientHandler(final FlightSqlClient sqlClient,
                              BufferAllocator allocator, final Collection<CallOption> options) {
    this.options.addAll(options);
    this.flightSqlClientPool = new KeyedFlightSqlClientObjectPool(
        new KeyedFlightSqlClientObjectPoolFactory(allocator));
    this.sqlClient = Preconditions.checkNotNull(sqlClient);
  }

  /**
   * Creates a new {@link ArrowFlightSqlClientHandler} from the provided {@code client} and {@code options}.
   *
   * @param client  the {@link FlightClient} to manage under a {@link FlightSqlClient} wrapper.
   * @param allocator the {@link BufferAllocator} used to create the client.
   * @param options the {@link CallOption}s to persist in between subsequent client calls.
   * @return a new {@link ArrowFlightSqlClientHandler}.
   */
  public static ArrowFlightSqlClientHandler createNewHandler(final FlightClient client,
                                                             final BufferAllocator allocator,
                                                             final Collection<CallOption> options) {
    return new ArrowFlightSqlClientHandler(new FlightSqlClient(client), allocator, options);
  }

  /**
   * Gets the {@link #options} for the subsequent calls from this handler.
   *
   * @return the {@link CallOption}s.
   */
  private CallOption[] getOptions() {
    return options.toArray(new CallOption[0]);
  }

  /**
   * Makes an RPC "getStream" request based on the provided {@link FlightInfo}
   * object. Retrieves the result of the query previously prepared with "getInfo."
   *
   * @param flightInfo The {@link FlightInfo} instance from which to fetch results.
   * @return a {@code FlightStream} of results.
   */
  public List<FlightStream> getStreams(final FlightInfo flightInfo) throws SQLException {
    final List<FlightStream> allStreams = new ArrayList<>();

    for (final FlightEndpoint endpoint : flightInfo.getEndpoints()) {
      List<Location> locations = endpoint.getLocations();
      if (locations.isEmpty()) {
        allStreams.add(sqlClient.getStream(endpoint.getTicket(), getOptions()));
        continue;
      }
      final Location location = locations.get(0); // purposefully discard other locations

      logger.info(String.format("Getting a client for location %s.", location));
      FlightSqlClient flightSqlClient;
      try {
        flightSqlClient = flightSqlClientPool.borrowObject(location);
      } catch (final NoSuchElementException e) {
        try {
          flightSqlClientPool.addObject(location);
          flightSqlClient = flightSqlClientPool.borrowObject(location);
        } catch (final Exception addObjectEx) {
          throw new SQLException("Failed to create and get a new FlightSqlClient in the ObjectPool.", addObjectEx);
        }
      } catch (final Exception borrowObjectEx) {
        throw new SQLException("Failed to borrow a FlightSqlClient from the ObjectPool", borrowObjectEx);
      }

      allStreams.add(flightSqlClient.getStream(endpoint.getTicket(), getOptions()));
      flightSqlClientPool.returnObject(location, flightSqlClient);
    }
    return allStreams;
  }

  /**
   * Makes an RPC "getInfo" request based on the provided {@code query}
   * object.
   *
   * @param query The query.
   * @return a {@code FlightStream} of results.
   */
  public FlightInfo getInfo(final String query) {
    return sqlClient.execute(query, getOptions());
  }

  @Override
  public void close() throws SQLException {
    try {
      AutoCloseables.close(sqlClient);
      flightSqlClientPool.close();
    } catch (final Exception e) {
      throw new SQLException("Failed to clean up client resources.", e);
    }
  }

  /**
   * A prepared statement handler.
   */
  public interface PreparedStatement extends AutoCloseable {
    /**
     * Executes this {@link PreparedStatement}.
     *
     * @return the {@link FlightInfo} representing the outcome of this query execution.
     * @throws SQLException on error.
     */
    FlightInfo executeQuery() throws SQLException;

    /**
     * Executes a {@link StatementType#UPDATE} query.
     *
     * @return the number of rows affected.
     */
    long executeUpdate();

    /**
     * Gets the {@link StatementType} of this {@link PreparedStatement}.
     *
     * @return the Statement Type.
     */
    StatementType getType();

    /**
     * Gets the {@link Schema} of this {@link PreparedStatement}.
     *
     * @return {@link Schema}.
     */
    Schema getDataSetSchema();

    @Override
    void close();
  }

  /**
   * Creates a new {@link PreparedStatement} for the given {@code query}.
   *
   * @param query the SQL query.
   * @return a new prepared statement.
   */
  public PreparedStatement prepare(final String query) {
    final FlightSqlClient.PreparedStatement preparedStatement =
        sqlClient.prepare(query, getOptions());
    return new PreparedStatement() {
      @Override
      public FlightInfo executeQuery() throws SQLException {
        return preparedStatement.execute(getOptions());
      }

      @Override
      public long executeUpdate() {
        return preparedStatement.executeUpdate(getOptions());
      }

      @Override
      public StatementType getType() {
        final Schema schema = preparedStatement.getResultSetSchema();
        return schema.getFields().isEmpty() ? StatementType.UPDATE : StatementType.SELECT;
      }

      @Override
      public Schema getDataSetSchema() {
        return preparedStatement.getResultSetSchema();
      }

      @Override
      public void close() {
        preparedStatement.close(getOptions());
      }
    };
  }

  /**
   * Makes an RPC "getCatalogs" request.
   *
   * @return a {@code FlightStream} of results.
   */
  public FlightInfo getCatalogs() {
    return sqlClient.getCatalogs(getOptions());
  }

  /**
   * Makes an RPC "getImportedKeys" request based on the provided info.
   *
   * @param catalog The catalog name. Must match the catalog name as it is stored in the database.
   *                Retrieves those without a catalog. Null means that the catalog name should not be used to
   *                narrow the search.
   * @param schema  The schema name. Must match the schema name as it is stored in the database.
   *                "" retrieves those without a schema. Null means that the schema name should not be used to narrow
   *                the search.
   * @param table   The table name. Must match the table name as it is stored in the database.
   * @return a {@code FlightStream} of results.
   */
  public FlightInfo getImportedKeys(final String catalog, final String schema, final String table) {
    return sqlClient.getImportedKeys(TableRef.of(catalog, schema, table), getOptions());
  }

  /**
   * Makes an RPC "getExportedKeys" request based on the provided info.
   *
   * @param catalog The catalog name. Must match the catalog name as it is stored in the database.
   *                Retrieves those without a catalog. Null means that the catalog name should not be used to
   *                narrow the search.
   * @param schema  The schema name. Must match the schema name as it is stored in the database.
   *                "" retrieves those without a schema. Null means that the schema name should not be used to narrow
   *                the search.
   * @param table   The table name. Must match the table name as it is stored in the database.
   * @return a {@code FlightStream} of results.
   */
  public FlightInfo getExportedKeys(final String catalog, final String schema, final String table) {
    return sqlClient.getExportedKeys(TableRef.of(catalog, schema, table), getOptions());
  }

  /**
   * Makes an RPC "getSchemas" request based on the provided info.
   *
   * @param catalog       The catalog name. Must match the catalog name as it is stored in the database.
   *                      Retrieves those without a catalog. Null means that the catalog name should not be used to
   *                      narrow the search.
   * @param schemaPattern The schema name pattern. Must match the schema name as it is stored in the database.
   *                      Null means that schema name should not be used to narrow down the search.
   * @return a {@code FlightStream} of results.
   */
  public FlightInfo getSchemas(final String catalog, final String schemaPattern) {
    return sqlClient.getSchemas(catalog, schemaPattern, getOptions());
  }

  /**
   * Makes an RPC "getTableTypes" request.
   *
   * @return a {@code FlightStream} of results.
   */
  public FlightInfo getTableTypes() {
    return sqlClient.getTableTypes(getOptions());
  }

  /**
   * Makes an RPC "getTables" request based on the provided info.
   *
   * @param catalog          The catalog name. Must match the catalog name as it is stored in the database.
   *                         Retrieves those without a catalog. Null means that the catalog name should not be used to
   *                         narrow the search.
   * @param schemaPattern    The schema name pattern. Must match the schema name as it is stored in the database.
   *                         "" retrieves those without a schema. Null means that the schema name should not be used to
   *                         narrow the search.
   * @param tableNamePattern The table name pattern. Must match the table name as it is stored in the database.
   * @param types            The list of table types, which must be from the list of table types to include.
   *                         Null returns all types.
   * @param includeSchema    Whether to include schema.
   * @return a {@code FlightStream} of results.
   */
  public FlightInfo getTables(final String catalog, final String schemaPattern,
                              final String tableNamePattern,
                              final List<String> types, final boolean includeSchema) {

    return sqlClient.getTables(catalog, schemaPattern, tableNamePattern, types, includeSchema,
        getOptions());
  }

  /**
   * Gets SQL info.
   *
   * @return the SQL info.
   */
  public FlightInfo getSqlInfo(SqlInfo... info) {
    return sqlClient.getSqlInfo(info, getOptions());
  }

  /**
   * Makes an RPC "getPrimaryKeys" request based on the provided info.
   *
   * @param catalog The catalog name; must match the catalog name as it is stored in the database.
   *                "" retrieves those without a catalog.
   *                Null means that the catalog name should not be used to narrow the search.
   * @param schema  The schema name; must match the schema name as it is stored in the database.
   *                "" retrieves those without a schema. Null means that the schema name should not be used to narrow
   *                the search.
   * @param table   The table name. Must match the table name as it is stored in the database.
   * @return a {@code FlightStream} of results.
   */
  public FlightInfo getPrimaryKeys(final String catalog, final String schema, final String table) {
    return sqlClient.getPrimaryKeys(TableRef.of(catalog, schema, table), getOptions());
  }

  /**
   * Makes an RPC "getCrossReference" request based on the provided info.
   *
   * @param pkCatalog The catalog name. Must match the catalog name as it is stored in the database.
   *                  Retrieves those without a catalog. Null means that the catalog name should not be used to
   *                  narrow the search.
   * @param pkSchema  The schema name. Must match the schema name as it is stored in the database.
   *                  "" retrieves those without a schema. Null means that the schema name should not be used to narrow
   *                  the search.
   * @param pkTable   The table name. Must match the table name as it is stored in the database.
   * @param fkCatalog The catalog name. Must match the catalog name as it is stored in the database.
   *                  Retrieves those without a catalog. Null means that the catalog name should not be used to
   *                  narrow the search.
   * @param fkSchema  The schema name. Must match the schema name as it is stored in the database.
   *                  "" retrieves those without a schema. Null means that the schema name should not be used to narrow
   *                  the search.
   * @param fkTable   The table name. Must match the table name as it is stored in the database.
   * @return a {@code FlightStream} of results.
   */
  public FlightInfo getCrossReference(String pkCatalog, String pkSchema, String pkTable,
                                      String fkCatalog, String fkSchema, String fkTable) {
    return sqlClient.getCrossReference(TableRef.of(pkCatalog, pkSchema, pkTable),
        TableRef.of(fkCatalog, fkSchema, fkTable),
        getOptions());
  }

  /**
   * Builder for {@link ArrowFlightSqlClientHandler}.
   */
  public static final class Builder {
    private final Set<FlightClientMiddleware.Factory> middlewareFactories = new HashSet<>();
    private final Set<CallOption> options = new HashSet<>();
    private String host;
    private int port;
    private String username;
    private String password;
    private String keyStorePath;
    private String keyStorePassword;
    private String token;
    private boolean useTls;
    private BufferAllocator allocator;

    /**
     * Sets the host for this handler.
     *
     * @param host the host.
     * @return this instance.
     */
    public Builder withHost(final String host) {
      this.host = host;
      return this;
    }

    /**
     * Sets the port for this handler.
     *
     * @param port the port.
     * @return this instance.
     */
    public Builder withPort(final int port) {
      this.port = port;
      return this;
    }

    /**
     * Sets the username for this handler.
     *
     * @param username the username.
     * @return this instance.
     */
    public Builder withUsername(final String username) {
      this.username = username;
      return this;
    }

    /**
     * Sets the password for this handler.
     *
     * @param password the password.
     * @return this instance.
     */
    public Builder withPassword(final String password) {
      this.password = password;
      return this;
    }

    /**
     * Sets the KeyStore path for this handler.
     *
     * @param keyStorePath the KeyStore path.
     * @return this instance.
     */
    public Builder withKeyStorePath(final String keyStorePath) {
      this.keyStorePath = keyStorePath;
      return this;
    }

    /**
     * Sets the KeyStore password for this handler.
     *
     * @param keyStorePassword the KeyStore password.
     * @return this instance.
     */
    public Builder withKeyStorePassword(final String keyStorePassword) {
      this.keyStorePassword = keyStorePassword;
      return this;
    }

    /**
     * Sets whether to use TLS encryption in this handler.
     *
     * @param useTls whether to use TLS encryption.
     * @return this instance.
     */
    public Builder withTlsEncryption(final boolean useTls) {
      this.useTls = useTls;
      return this;
    }

    /**
     * Sets the token used in the token authetication.
     * @param token the token value.
     * @return      this builder instance.
     */
    public Builder withToken(final String token) {
      this.token = token;
      return this;
    }

    /**
     * Sets the {@link BufferAllocator} to use in this handler.
     *
     * @param allocator the allocator.
     * @return this instance.
     */
    public Builder withBufferAllocator(final BufferAllocator allocator) {
      this.allocator = allocator
          .newChildAllocator("ArrowFlightSqlClientHandler", 0, allocator.getLimit());
      return this;
    }

    /**
     * Adds the provided {@code factories} to the list of {@link #middlewareFactories} of this handler.
     *
     * @param factories the factories to add.
     * @return this instance.
     */
    public Builder withMiddlewareFactories(final FlightClientMiddleware.Factory... factories) {
      return withMiddlewareFactories(Arrays.asList(factories));
    }

    /**
     * Adds the provided {@code factories} to the list of {@link #middlewareFactories} of this handler.
     *
     * @param factories the factories to add.
     * @return this instance.
     */
    public Builder withMiddlewareFactories(
        final Collection<FlightClientMiddleware.Factory> factories) {
      this.middlewareFactories.addAll(factories);
      return this;
    }

    /**
     * Adds the provided {@link CallOption}s to this handler.
     *
     * @param options the options
     * @return this instance.
     */
    public Builder withCallOptions(final CallOption... options) {
      return withCallOptions(Arrays.asList(options));
    }

    /**
     * Adds the provided {@link CallOption}s to this handler.
     *
     * @param options the options
     * @return this instance.
     */
    public Builder withCallOptions(final Collection<CallOption> options) {
      this.options.addAll(options);
      return this;
    }

    /**
     * Builds a new {@link ArrowFlightSqlClientHandler} from the provided fields.
     *
     * @return a new client handler.
     * @throws SQLException on error.
     */
    public ArrowFlightSqlClientHandler build() throws SQLException {
      FlightClient client = null;
      try {
        ClientIncomingAuthHeaderMiddleware.Factory authFactory = null;
        if (username != null) {
          authFactory =
              new ClientIncomingAuthHeaderMiddleware.Factory(new ClientBearerHeaderHandler());
          withMiddlewareFactories(authFactory);
        }
        final FlightClient.Builder clientBuilder = FlightClient.builder().allocator(allocator);
        withMiddlewareFactories(new ClientCookieMiddleware.Factory());
        middlewareFactories.forEach(clientBuilder::intercept);
        Location location;
        if (useTls) {
          location = Location.forGrpcTls(host, port);
          clientBuilder.useTls();
        } else {
          location = Location.forGrpcInsecure(host, port);
        }
        clientBuilder.location(location);
        if (keyStorePath != null) {
          clientBuilder.trustedCertificates(
              ClientAuthenticationUtils.getCertificateStream(keyStorePath, keyStorePassword));
        }
        client = clientBuilder.build();
        if (authFactory != null) {
          options.add(
              ClientAuthenticationUtils.getAuthenticate(client, username, password, authFactory));
        } else if (token != null) {
          options.add(
              ClientAuthenticationUtils.getAuthenticate(
                  client, new CredentialCallOption(new BearerCredentialWriter(token))));
        }
        return ArrowFlightSqlClientHandler.createNewHandler(client, allocator, options);

      } catch (final IllegalArgumentException | GeneralSecurityException | IOException | FlightRuntimeException e) {
        final SQLException originalException = new SQLException(e);
        if (client != null) {
          try {
            client.close();
          } catch (final InterruptedException interruptedException) {
            originalException.addSuppressed(interruptedException);
          }
        }
        throw originalException;
      }
    }
  }
}
