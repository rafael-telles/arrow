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

package org.apache.arrow.flight;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Objects.isNull;
import static org.apache.arrow.util.AutoCloseables.close;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.FlightSqlClient.PreparedStatement;
import org.apache.arrow.flight.sql.FlightSqlExample;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.hamcrest.Matcher;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

import com.google.common.collect.ImmutableList;

/**
 * Test direct usage of Flight SQL workflows.
 */
public class TestFlightSql {

  protected static final Schema SCHEMA_INT_TABLE = new Schema(asList(
      new Field("ID", new FieldType(false, MinorType.INT.getType(), null), null),
      Field.nullable("KEYNAME", MinorType.VARCHAR.getType()),
      Field.nullable("VALUE", MinorType.INT.getType()),
      Field.nullable("FOREIGNID", MinorType.INT.getType())));
  private static final List<List<String>> EXPECTED_RESULTS_FOR_STAR_SELECT_QUERY = ImmutableList.of(
      asList("1", "one", "1", "1"), asList("2", "zero", "0", "1"), asList("3", "negative one", "-1", "1"));
  private static final Map<String, String> GET_SQL_INFO_EXPECTED_RESULTS_MAP = new LinkedHashMap<>();
  private static final String LOCALHOST = "localhost";
  private static final int[] ALL_SQL_INFO_ARGS = {
      FlightSqlProducer.SqlInfo.FLIGHT_SQL_SERVER_NAME,
      FlightSqlProducer.SqlInfo.FLIGHT_SQL_SERVER_VERSION,
      FlightSqlProducer.SqlInfo.FLIGHT_SQL_SERVER_ARROW_VERSION,
      FlightSqlProducer.SqlInfo.FLIGHT_SQL_SERVER_READ_ONLY,
      FlightSqlProducer.SqlInfo.SQL_DDL_CATALOG,
      FlightSqlProducer.SqlInfo.SQL_DDL_SCHEMA,
      FlightSqlProducer.SqlInfo.SQL_DDL_TABLE,
      FlightSqlProducer.SqlInfo.SQL_IDENTIFIER_CASE,
      FlightSqlProducer.SqlInfo.SQL_IDENTIFIER_QUOTE_CHAR,
      FlightSqlProducer.SqlInfo.SQL_QUOTED_IDENTIFIER_CASE};
  private static int port;
  private static BufferAllocator allocator;
  private static FlightServer server;
  private static FlightClient client;
  private static FlightSqlClient sqlClient;
  @Rule
  public final ErrorCollector collector = new ErrorCollector();

  @BeforeClass
  public static void setUp() throws Exception {
    try (final Reader reader = new BufferedReader(
        new FileReader("target/generated-test-resources/network.properties"))) {
      final Properties properties = new Properties();
      properties.load(reader);
      port = Integer.parseInt(Objects.toString(properties.get("server.port")));
    }

    allocator = new RootAllocator(Integer.MAX_VALUE);

    final Location serverLocation = Location.forGrpcInsecure(LOCALHOST, port);
    server = FlightServer.builder(allocator, serverLocation, new FlightSqlExample(serverLocation))
        .build()
        .start();

    final Location clientLocation = Location.forGrpcInsecure(LOCALHOST, server.getPort());
    client = FlightClient.builder(allocator, clientLocation).build();
    sqlClient = new FlightSqlClient(client);

    GET_SQL_INFO_EXPECTED_RESULTS_MAP
        .put(Integer.toString(ALL_SQL_INFO_ARGS[0]), "Apache Derby");
    GET_SQL_INFO_EXPECTED_RESULTS_MAP
        .put(Integer.toString(ALL_SQL_INFO_ARGS[1]), "10.14.2.0 - (1828579)");
    GET_SQL_INFO_EXPECTED_RESULTS_MAP
        .put(Integer.toString(ALL_SQL_INFO_ARGS[2]), "10.14.2.0 - (1828579)");
    GET_SQL_INFO_EXPECTED_RESULTS_MAP
        .put(Integer.toString(ALL_SQL_INFO_ARGS[3]), "0");
    GET_SQL_INFO_EXPECTED_RESULTS_MAP
        .put(Integer.toString(ALL_SQL_INFO_ARGS[4]), "0");
    GET_SQL_INFO_EXPECTED_RESULTS_MAP
        .put(Integer.toString(ALL_SQL_INFO_ARGS[5]), "1");
    GET_SQL_INFO_EXPECTED_RESULTS_MAP
        .put(Integer.toString(ALL_SQL_INFO_ARGS[6]), "1");
    GET_SQL_INFO_EXPECTED_RESULTS_MAP
        .put(Integer.toString(ALL_SQL_INFO_ARGS[7]), "UPPERCASE");
    GET_SQL_INFO_EXPECTED_RESULTS_MAP
        .put(Integer.toString(ALL_SQL_INFO_ARGS[8]), "\"");
    GET_SQL_INFO_EXPECTED_RESULTS_MAP
        .put(Integer.toString(ALL_SQL_INFO_ARGS[9]), "CASE_INSENSITIVE");
  }

  @AfterClass
  public static void tearDown() throws Exception {
    close(client, server, allocator);
  }

  private static List<List<String>> getNonConformingResultsForGetSqlInfo(final List<? extends List<String>> results) {
    return getNonConformingResultsForGetSqlInfo(results, ALL_SQL_INFO_ARGS);
  }

  private static List<List<String>> getNonConformingResultsForGetSqlInfo(
      final List<? extends List<String>> results,
      final int... args) {
    final List<List<String>> nonConformingResults = new ArrayList<>();
    if (results.size() == args.length) {
      for (int index = 0; index < results.size(); index++) {
        final List<String> result = results.get(index);
        final String providedName = result.get(0);
        final String expectedName = Integer.toString(args[index]);
        if (!(GET_SQL_INFO_EXPECTED_RESULTS_MAP.get(providedName).equals(result.get(1)) &&
            providedName.equals(expectedName))) {
          nonConformingResults.add(result);
          break;
        }
      }
    }
    return nonConformingResults;
  }

  @Test
  public void testGetTablesSchema() {
    final FlightInfo info = sqlClient.getTables(null, null, null, null, true);
    collector.checkThat(info.getSchema(), is(FlightSqlProducer.Schemas.GET_TABLES_SCHEMA));
  }

  @Test
  public void testGetTablesResultNoSchema() throws Exception {
    try (final FlightStream stream =
             sqlClient.getStream(
                 sqlClient.getTables(null, null, null, null, false)
                     .getEndpoints().get(0).getTicket())) {
      collector.checkThat(stream.getSchema(), is(FlightSqlProducer.Schemas.GET_TABLES_SCHEMA_NO_SCHEMA));
      final List<List<String>> results = getResults(stream);
      final List<List<String>> expectedResults = ImmutableList.of(
          // catalog_name | schema_name | table_name | table_type | table_schema
          asList(null /* TODO No catalog yet */, "SYS", "SYSALIASES", "SYSTEM TABLE"),
          asList(null /* TODO No catalog yet */, "SYS", "SYSCHECKS", "SYSTEM TABLE"),
          asList(null /* TODO No catalog yet */, "SYS", "SYSCOLPERMS", "SYSTEM TABLE"),
          asList(null /* TODO No catalog yet */, "SYS", "SYSCOLUMNS", "SYSTEM TABLE"),
          asList(null /* TODO No catalog yet */, "SYS", "SYSCONGLOMERATES", "SYSTEM TABLE"),
          asList(null /* TODO No catalog yet */, "SYS", "SYSCONSTRAINTS", "SYSTEM TABLE"),
          asList(null /* TODO No catalog yet */, "SYS", "SYSDEPENDS", "SYSTEM TABLE"),
          asList(null /* TODO No catalog yet */, "SYS", "SYSFILES", "SYSTEM TABLE"),
          asList(null /* TODO No catalog yet */, "SYS", "SYSFOREIGNKEYS", "SYSTEM TABLE"),
          asList(null /* TODO No catalog yet */, "SYS", "SYSKEYS", "SYSTEM TABLE"),
          asList(null /* TODO No catalog yet */, "SYS", "SYSPERMS", "SYSTEM TABLE"),
          asList(null /* TODO No catalog yet */, "SYS", "SYSROLES", "SYSTEM TABLE"),
          asList(null /* TODO No catalog yet */, "SYS", "SYSROUTINEPERMS", "SYSTEM TABLE"),
          asList(null /* TODO No catalog yet */, "SYS", "SYSSCHEMAS", "SYSTEM TABLE"),
          asList(null /* TODO No catalog yet */, "SYS", "SYSSEQUENCES", "SYSTEM TABLE"),
          asList(null /* TODO No catalog yet */, "SYS", "SYSSTATEMENTS", "SYSTEM TABLE"),
          asList(null /* TODO No catalog yet */, "SYS", "SYSSTATISTICS", "SYSTEM TABLE"),
          asList(null /* TODO No catalog yet */, "SYS", "SYSTABLEPERMS", "SYSTEM TABLE"),
          asList(null /* TODO No catalog yet */, "SYS", "SYSTABLES", "SYSTEM TABLE"),
          asList(null /* TODO No catalog yet */, "SYS", "SYSTRIGGERS", "SYSTEM TABLE"),
          asList(null /* TODO No catalog yet */, "SYS", "SYSUSERS", "SYSTEM TABLE"),
          asList(null /* TODO No catalog yet */, "SYS", "SYSVIEWS", "SYSTEM TABLE"),
          asList(null /* TODO No catalog yet */, "SYSIBM", "SYSDUMMY1", "SYSTEM TABLE"),
          asList(null /* TODO No catalog yet */, "APP", "FOREIGNTABLE", "TABLE"),
          asList(null /* TODO No catalog yet */, "APP", "INTTABLE", "TABLE"));
      collector.checkThat(results, is(expectedResults));
    }
  }

  @Test
  public void testGetTablesResultFilteredNoSchema() throws Exception {
    try (final FlightStream stream =
             sqlClient.getStream(
                 sqlClient.getTables(null, null, null, singletonList("TABLE"), false)
                     .getEndpoints().get(0).getTicket())) {
      collector.checkThat(stream.getSchema(), is(FlightSqlProducer.Schemas.GET_TABLES_SCHEMA_NO_SCHEMA));
      final List<List<String>> results = getResults(stream);
      final List<List<String>> expectedResults = ImmutableList.of(
          // catalog_name | schema_name | table_name | table_type | table_schema
          asList(null /* TODO No catalog yet */, "APP", "FOREIGNTABLE", "TABLE"),
          asList(null /* TODO No catalog yet */, "APP", "INTTABLE", "TABLE"));
      collector.checkThat(results, is(expectedResults));
    }
  }

  @Test
  public void testGetTablesResultFilteredWithSchema() throws Exception {
    try (final FlightStream stream =
             sqlClient.getStream(
                 sqlClient.getTables(null, null, null, singletonList("TABLE"), true)
                     .getEndpoints().get(0).getTicket())) {
      collector.checkThat(stream.getSchema(), is(FlightSqlProducer.Schemas.GET_TABLES_SCHEMA));
      final List<List<String>> results = getResults(stream);
      final List<List<String>> expectedResults = ImmutableList.of(
          // catalog_name | schema_name | table_name | table_type | table_schema
          asList(
              null /* TODO No catalog yet */,
              "APP",
              "FOREIGNTABLE",
              "TABLE",
              new Schema(asList(
                  new Field("ID", new FieldType(false, MinorType.INT.getType(), null), null),
                  Field.nullable("FOREIGNNAME", MinorType.VARCHAR.getType()),
                  Field.nullable("VALUE", MinorType.INT.getType()))).toJson()),
          asList(
              null /* TODO No catalog yet */,
              "APP",
              "INTTABLE",
              "TABLE",
              new Schema(asList(
                  new Field("ID", new FieldType(false, MinorType.INT.getType(), null), null),
                  Field.nullable("KEYNAME", MinorType.VARCHAR.getType()),
                  Field.nullable("VALUE", MinorType.INT.getType()),
                  Field.nullable("FOREIGNID", MinorType.INT.getType()))).toJson()));
      collector.checkThat(results, is(expectedResults));
    }
  }

  @Test
  public void testSimplePreparedStatementSchema() throws Exception {
    try (final PreparedStatement preparedStatement = sqlClient.prepare("SELECT * FROM intTable")) {
      final Schema actualSchema = preparedStatement.getResultSetSchema();
      collector.checkThat(actualSchema, is(SCHEMA_INT_TABLE));

      final FlightInfo info = preparedStatement.execute();
      collector.checkThat(info.getSchema(), is(SCHEMA_INT_TABLE));
    }
  }

  @Test
  public void testSimplePreparedStatementResults() throws Exception {
    try (final FlightStream stream =
             sqlClient.getStream(
                 sqlClient.prepare("SELECT * FROM intTable")
                     .execute()
                     .getEndpoints()
                     .get(0).getTicket())) {
      collector.checkThat(stream.getSchema(), is(SCHEMA_INT_TABLE));
      collector.checkThat(getResults(stream), is(EXPECTED_RESULTS_FOR_STAR_SELECT_QUERY));
    }
  }

  @Test
  public void testSimplePreparedStatementClosesProperly() {
    final PreparedStatement preparedStatement = sqlClient.prepare("SELECT * FROM intTable");
    collector.checkThat(preparedStatement.isClosed(), is(false));
    preparedStatement.close();
    collector.checkThat(preparedStatement.isClosed(), is(true));
  }

  @Test
  public void testGetCatalogsSchema() {
    final FlightInfo info = sqlClient.getCatalogs();
    collector.checkThat(info.getSchema(), is(FlightSqlProducer.Schemas.GET_CATALOGS_SCHEMA));
  }

  @Test
  public void testGetCatalogsResults() throws Exception {
    try (final FlightStream stream =
             sqlClient.getStream(sqlClient.getCatalogs().getEndpoints().get(0).getTicket())) {
      collector.checkThat(stream.getSchema(), is(FlightSqlProducer.Schemas.GET_CATALOGS_SCHEMA));
      List<List<String>> catalogs = getResults(stream);
      collector.checkThat(catalogs, is(emptyList()));
    }
  }

  @Test
  public void testGetTableTypesSchema() {
    final FlightInfo info = sqlClient.getTableTypes();
    collector.checkThat(info.getSchema(), is(FlightSqlProducer.Schemas.GET_TABLE_TYPES_SCHEMA));
  }

  @Test
  public void testGetTableTypesResult() throws Exception {
    try (final FlightStream stream =
             sqlClient.getStream(sqlClient.getTableTypes().getEndpoints().get(0).getTicket())) {
      collector.checkThat(stream.getSchema(), is(FlightSqlProducer.Schemas.GET_TABLE_TYPES_SCHEMA));
      final List<List<String>> tableTypes = getResults(stream);
      final List<List<String>> expectedTableTypes = ImmutableList.of(
          // table_type
          singletonList("SYNONYM"),
          singletonList("SYSTEM TABLE"),
          singletonList("TABLE"),
          singletonList("VIEW")
      );
      collector.checkThat(tableTypes, is(expectedTableTypes));
    }
  }

  @Test
  public void testGetSchemasSchema() {
    final FlightInfo info = sqlClient.getSchemas(null, null);
    collector.checkThat(info.getSchema(), is(FlightSqlProducer.Schemas.GET_SCHEMAS_SCHEMA));
  }

  @Test
  public void testGetSchemasResult() throws Exception {
    try (final FlightStream stream =
             sqlClient.getStream(sqlClient.getSchemas(null, null).getEndpoints().get(0).getTicket())) {
      collector.checkThat(stream.getSchema(), is(FlightSqlProducer.Schemas.GET_SCHEMAS_SCHEMA));
      final List<List<String>> schemas = getResults(stream);
      final List<List<String>> expectedSchemas = ImmutableList.of(
          // catalog_name | schema_name
          asList(null /* TODO Add catalog. */, "APP"),
          asList(null /* TODO Add catalog. */, "NULLID"),
          asList(null /* TODO Add catalog. */, "SQLJ"),
          asList(null /* TODO Add catalog. */, "SYS"),
          asList(null /* TODO Add catalog. */, "SYSCAT"),
          asList(null /* TODO Add catalog. */, "SYSCS_DIAG"),
          asList(null /* TODO Add catalog. */, "SYSCS_UTIL"),
          asList(null /* TODO Add catalog. */, "SYSFUN"),
          asList(null /* TODO Add catalog. */, "SYSIBM"),
          asList(null /* TODO Add catalog. */, "SYSPROC"),
          asList(null /* TODO Add catalog. */, "SYSSTAT"));
      collector.checkThat(schemas, is(expectedSchemas));
    }
  }

  @Test
  public void testGetPrimaryKey() {
    final FlightInfo flightInfo = sqlClient.getPrimaryKeys(null, null, "INTTABLE");
    final FlightStream stream = sqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket());

    final List<List<String>> results = getResults(stream);
    collector.checkThat(results.size(), is(1));

    final List<String> result = results.get(0);

    collector.checkThat(result.get(0), is(""));
    collector.checkThat(result.get(1), is("APP"));
    collector.checkThat(result.get(2), is("INTTABLE"));
    collector.checkThat(result.get(3), is("ID"));
    collector.checkThat(result.get(4), is("1"));
    collector.checkThat(result.get(5), notNullValue());
  }

  @Test
  public void testGetSqlInfoSchema() {
    final FlightInfo info = sqlClient.getSqlInfo();
    collector.checkThat(info.getSchema(), is(FlightSqlProducer.Schemas.GET_SQL_INFO_SCHEMA));
  }

  @Test
  public void testGetSqlInfoResults() throws Exception {
    final FlightInfo info = sqlClient.getSqlInfo();
    try (final FlightStream stream = sqlClient.getStream(info.getEndpoints().get(0).getTicket())) {
      collector.checkThat(stream.getSchema(), is(FlightSqlProducer.Schemas.GET_SQL_INFO_SCHEMA));
      collector.checkThat(getNonConformingResultsForGetSqlInfo(getResults(stream)), is(emptyList()));
    }
  }

  @Test
  public void testGetSqlInfoResultsWithSingleArg() throws Exception {
    final int arg = FlightSqlProducer.SqlInfo.FLIGHT_SQL_SERVER_NAME;
    final FlightInfo info = sqlClient.getSqlInfo(arg);
    try (final FlightStream stream = sqlClient.getStream(info.getEndpoints().get(0).getTicket())) {
      collector.checkThat(stream.getSchema(), is(FlightSqlProducer.Schemas.GET_SQL_INFO_SCHEMA));
      collector.checkThat(getNonConformingResultsForGetSqlInfo(getResults(stream), arg), is(emptyList()));
    }
  }

  @Test
  public void testGetSqlInfoResultsWithTwoArgs() throws Exception {
    final int[] args = {
        FlightSqlProducer.SqlInfo.FLIGHT_SQL_SERVER_NAME,
        FlightSqlProducer.SqlInfo.FLIGHT_SQL_SERVER_VERSION};
    final FlightInfo info = sqlClient.getSqlInfo(args);
    try (final FlightStream stream = sqlClient.getStream(info.getEndpoints().get(0).getTicket())) {
      collector.checkThat(stream.getSchema(), is(FlightSqlProducer.Schemas.GET_SQL_INFO_SCHEMA));
      collector.checkThat(getNonConformingResultsForGetSqlInfo(getResults(stream), args), is(emptyList()));
    }
  }

  @Test
  public void testGetSqlInfoResultsWithThreeArgs() throws Exception {
    final int[] args = {
        FlightSqlProducer.SqlInfo.FLIGHT_SQL_SERVER_NAME,
        FlightSqlProducer.SqlInfo.FLIGHT_SQL_SERVER_VERSION,
        FlightSqlProducer.SqlInfo.SQL_IDENTIFIER_QUOTE_CHAR};
    final FlightInfo info = sqlClient.getSqlInfo(args);
    try (final FlightStream stream = sqlClient.getStream(info.getEndpoints().get(0).getTicket())) {
      collector.checkThat(stream.getSchema(), is(FlightSqlProducer.Schemas.GET_SQL_INFO_SCHEMA));
      collector.checkThat(getNonConformingResultsForGetSqlInfo(getResults(stream), args), is(emptyList()));
    }
  }

  @Test
  public void testGetCommandExportedKeys() {
    final FlightStream stream =
        sqlClient.getStream(
            sqlClient.getExportedKeys(null, null, "FOREIGNTABLE")
                .getEndpoints().get(0).getTicket());

    final List<List<String>> results = getResults(stream);

    final List<Matcher<String>> matchers = asList(
        nullValue(String.class), // pk_catalog_name
        is("APP"), // pk_schema_name
        is("FOREIGNTABLE"), // pk_table_name
        is("ID"), // pk_column_name
        nullValue(String.class), // fk_catalog_name
        is("APP"), // fk_schema_name
        is("INTTABLE"), // fk_table_name
        is("FOREIGNID"), // fk_column_name
        is("1"), // key_sequence
        containsString("SQL"), // fk_key_name
        containsString("SQL"), // pk_key_name
        is("3"), // update_rule
        is("3")); // delete_rule

    Assert.assertEquals(1, results.size());
    for (int i = 0; i < matchers.size(); i++) {
      collector.checkThat(results.get(0).get(i), matchers.get(i));
    }
  }

  @Test
  public void testGetCommandImportedKeys() {
    final FlightStream stream =
        sqlClient.getStream(
            sqlClient.getImportedKeys(null, null, "INTTABLE")
                .getEndpoints().get(0).getTicket());

    final List<List<String>> results = getResults(stream);

    final List<Matcher<String>> matchers = asList(
        nullValue(String.class), // pk_catalog_name
        is("APP"), // pk_schema_name
        is("FOREIGNTABLE"), // pk_table_name
        is("ID"), // pk_column_name
        nullValue(String.class), // fk_catalog_name
        is("APP"), // fk_schema_name
        is("INTTABLE"), // fk_table_name
        is("FOREIGNID"), // fk_column_name
        is("1"), // key_sequence
        containsString("SQL"), // fk_key_name
        containsString("SQL"), // pk_key_name
        is("3"), // update_rule
        is("3")); // delete_rule

    Assert.assertEquals(1, results.size());
    for (int i = 0; i < matchers.size(); i++) {
      collector.checkThat(results.get(0).get(i), matchers.get(i));
    }
  }

  @Test
  public void testCreateStatementSchema() {
    final FlightInfo info = sqlClient.execute("SELECT * FROM intTable");
    collector.checkThat(info.getSchema(), is(SCHEMA_INT_TABLE));
  }

  @Test
  public void testCreateStatementResults() throws Exception {
    try (final FlightStream stream = sqlClient
        .getStream(sqlClient.execute("SELECT * FROM intTable").getEndpoints().get(0).getTicket())) {
      collector.checkThat(stream.getSchema(), is(SCHEMA_INT_TABLE));
      collector.checkThat(getResults(stream), is(EXPECTED_RESULTS_FOR_STAR_SELECT_QUERY));
    }
  }

  List<List<String>> getResults(FlightStream stream) {
    final List<List<String>> results = new ArrayList<>();
    while (stream.next()) {
      try (final VectorSchemaRoot root = stream.getRoot()) {
        final long rowCount = root.getRowCount();
        for (int i = 0; i < rowCount; ++i) {
          results.add(new ArrayList<>());
        }

        root.getSchema().getFields().forEach(field -> {
          try (final FieldVector fieldVector = root.getVector(field.getName())) {
            if (fieldVector instanceof VarCharVector) {
              final VarCharVector varcharVector = (VarCharVector) fieldVector;
              for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                final Text data = varcharVector.getObject(rowIndex);
                results.get(rowIndex).add(isNull(data) ? null : data.toString());
              }
            } else if (fieldVector instanceof IntVector) {
              for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                results.get(rowIndex).add(String.valueOf(((IntVector) fieldVector).get(rowIndex)));
              }
            } else if (fieldVector instanceof VarBinaryVector) {
              final VarBinaryVector varbinaryVector = (VarBinaryVector) fieldVector;
              for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                final byte[] data = varbinaryVector.getObject(rowIndex);
                final String output =
                    isNull(data) ? null : Schema.deserialize(ByteBuffer.wrap(data)).toJson();
                results.get(rowIndex).add(output);
              }
            } else if (fieldVector instanceof DenseUnionVector) {
              final DenseUnionVector denseUnionVector = (DenseUnionVector) fieldVector;
              for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                final Object data = denseUnionVector.getObject(rowIndex);
                results.get(rowIndex).add(isNull(data) ? null : Objects.toString(data));
              }
            } else {
              throw new UnsupportedOperationException("Not yet implemented");
            }
          }
        });
      }
    }

    return results;
  }

  @Test
  public void testExecuteUpdate() {
    long insertedCount = sqlClient.executeUpdate("INSERT INTO INTTABLE (keyName, value) VALUES " +
        "('KEYNAME1', 1001), ('KEYNAME2', 1002), ('KEYNAME3', 1003)");
    collector.checkThat(insertedCount, is(3L));

    long updatedCount = sqlClient.executeUpdate("UPDATE INTTABLE SET keyName = 'KEYNAME1' " +
        "WHERE keyName = 'KEYNAME2' OR keyName = 'KEYNAME3'");
    collector.checkThat(updatedCount, is(2L));

    long deletedCount = sqlClient.executeUpdate("DELETE FROM INTTABLE WHERE keyName = 'KEYNAME1'");
    collector.checkThat(deletedCount, is(3L));
  }

  @Test
  public void testQueryWithNoResultsShouldNotHang() throws Exception {
    try (final PreparedStatement preparedStatement = sqlClient.prepare("SELECT * FROM intTable WHERE 1 = 0");
         final FlightStream stream = sqlClient
             .getStream(preparedStatement.execute().getEndpoints().get(0).getTicket())) {
      collector.checkThat(stream.getSchema(), is(SCHEMA_INT_TABLE));

      final List<List<String>> result = getResults(stream);
      collector.checkThat(result, is(emptyList()));
    }
  }
}
