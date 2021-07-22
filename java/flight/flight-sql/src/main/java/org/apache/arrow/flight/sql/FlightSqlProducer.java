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

package org.apache.arrow.flight.sql;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionClosePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetCatalogs;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetForeignKeys;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetPrimaryKeys;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetSchemas;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetSqlInfo;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTableTypes;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTables;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementUpdate;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementUpdate;
import org.apache.arrow.flight.sql.impl.FlightSql.DoPutUpdateResult;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType.Union;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;

import io.grpc.Status;

/**
 * API to Implement an Arrow Flight SQL producer.
 */
public abstract class FlightSqlProducer implements FlightProducer, AutoCloseable {
  protected static final Schema GET_TABLES_SCHEMA = new Schema(Arrays.asList(
      Field.nullable("catalog_name", MinorType.VARCHAR.getType()),
      Field.nullable("schema_name", MinorType.VARCHAR.getType()),
      Field.nullable("table_name", MinorType.VARCHAR.getType()),
      Field.nullable("table_type", MinorType.VARCHAR.getType()),
      Field.nullable("table_schema", MinorType.VARBINARY.getType())));
  protected static final Schema GET_TABLES_SCHEMA_NO_SCHEMA = new Schema(Arrays.asList(
      Field.nullable("catalog_name", MinorType.VARCHAR.getType()),
      Field.nullable("schema_name", MinorType.VARCHAR.getType()),
      Field.nullable("table_name", MinorType.VARCHAR.getType()),
      Field.nullable("table_type", MinorType.VARCHAR.getType())));
  protected static final Schema GET_CATALOGS_SCHEMA = new Schema(
      Collections.singletonList(new Field("catalog_name", FieldType.nullable(MinorType.VARCHAR.getType()), null)));
  protected static final Schema GET_TABLE_TYPES_SCHEMA =
      new Schema(Collections.singletonList(Field.nullable("table_type", MinorType.VARCHAR.getType())));
  protected static final Schema GET_SCHEMAS_SCHEMA = new Schema(
      Arrays.asList(Field.nullable("catalog_name", MinorType.VARCHAR.getType()),
          Field.nullable("schema_name", MinorType.VARCHAR.getType())));

  /**
   * Depending on the provided command, method either:
   * 1. Return information about a SQL query, or
   * 2. Return information about a prepared statement. In this case, parameters binding is allowed.
   *
   * @param context    Per-call context.
   * @param descriptor The descriptor identifying the data stream.
   * @return information about the given SQL query, or the given prepared statement.
   */
  @Override
  public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
    final Any command = FlightSqlUtils.parseOrThrow(descriptor.getCommand());

    if (command.is(CommandStatementQuery.class)) {
      return getFlightInfoStatement(
          FlightSqlUtils.unpackOrThrow(command, CommandStatementQuery.class), context, descriptor);
    } else if (command.is(CommandPreparedStatementQuery.class)) {
      return getFlightInfoPreparedStatement(
          FlightSqlUtils.unpackOrThrow(command, CommandPreparedStatementQuery.class), context, descriptor);
    } else if (command.is(CommandGetCatalogs.class)) {
      return getFlightInfoCatalogs(
          FlightSqlUtils.unpackOrThrow(command, CommandGetCatalogs.class), context, descriptor);
    } else if (command.is(CommandGetSchemas.class)) {
      return getFlightInfoSchemas(
          FlightSqlUtils.unpackOrThrow(command, CommandGetSchemas.class), context, descriptor);
    } else if (command.is(CommandGetTables.class)) {
      return getFlightInfoTables(
          FlightSqlUtils.unpackOrThrow(command, CommandGetTables.class), context, descriptor);
    } else if (command.is(CommandGetTableTypes.class)) {
      return getFlightInfoTableTypes(
          FlightSqlUtils.unpackOrThrow(command, CommandGetTableTypes.class), context, descriptor);
    } else if (command.is(CommandGetSqlInfo.class)) {
      return getFlightInfoSqlInfo(
          FlightSqlUtils.unpackOrThrow(command, CommandGetSqlInfo.class), context, descriptor);
    } else if (command.is(CommandGetPrimaryKeys.class)) {
      return getFlightInfoPrimaryKeys(
          FlightSqlUtils.unpackOrThrow(command, CommandGetPrimaryKeys.class), context, descriptor);
    } else if (command.is(CommandGetForeignKeys.class)) {
      return getFlightInfoForeignKeys(
          FlightSqlUtils.unpackOrThrow(command, CommandGetForeignKeys.class), context, descriptor);
    }

    throw Status.INVALID_ARGUMENT.asRuntimeException();
  }

  /**
   * Returns the schema of the result produced by the SQL query.
   *
   * @param context    Per-call context.
   * @param descriptor The descriptor identifying the data stream.
   * @return the result set schema.
   */
  @Override
  public SchemaResult getSchema(CallContext context, FlightDescriptor descriptor) {
    final Any command = FlightSqlUtils.parseOrThrow(descriptor.getCommand());

    if (command.is(CommandStatementQuery.class)) {
      return getSchemaStatement(
          FlightSqlUtils.unpackOrThrow(command, CommandStatementQuery.class), context, descriptor);
    } else if (command.is(CommandGetCatalogs.class)) {
      return getSchemaCatalogs();
    } else if (command.is(CommandGetSchemas.class)) {
      return getSchemaSchemas();
    } else if (command.is(CommandGetTables.class)) {
      return getSchemaTables();
    } else if (command.is(CommandGetTableTypes.class)) {
      return getSchemaTableTypes();
    } else if (command.is(CommandGetSqlInfo.class)) {
      return getSchemaSqlInfo();
    } else if (command.is(CommandGetPrimaryKeys.class)) {
      return getSchemaPrimaryKeys();
    } else if (command.is(CommandGetForeignKeys.class)) {
      return getSchemaForeignKeys();
    }

    throw Status.INVALID_ARGUMENT.asRuntimeException();
  }

  /**
   * Depending on the provided command, method either:
   * 1. Return data for a stream produced by executing the provided SQL query, or
   * 2. Return data for a prepared statement. In this case, parameters binding is allowed.
   *
   * @param context  Per-call context.
   * @param ticket   The application-defined ticket identifying this stream.
   * @param listener An interface for sending data back to the client.
   */
  @Override
  public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
    final Any command;

    try {
      command = Any.parseFrom(ticket.getBytes());
    } catch (InvalidProtocolBufferException e) {
      listener.error(e);
      return;
    }

    if (command.is(CommandStatementQuery.class)) {
      getStreamStatement(
          FlightSqlUtils.unpackOrThrow(command, CommandStatementQuery.class), context, ticket, listener);
    } else if (command.is(CommandPreparedStatementQuery.class)) {
      getStreamPreparedStatement(
          FlightSqlUtils.unpackOrThrow(command, CommandPreparedStatementQuery.class), context, ticket, listener);
    } else if (command.is(CommandGetCatalogs.class)) {
      getStreamCatalogs(context, ticket, listener);
    } else if (command.is(CommandGetSchemas.class)) {
      getStreamSchemas(FlightSqlUtils.unpackOrThrow(command, CommandGetSchemas.class), context, ticket, listener);
    } else if (command.is(CommandGetTables.class)) {
      getStreamTables(FlightSqlUtils.unpackOrThrow(command, CommandGetTables.class), context, ticket, listener);
    } else if (command.is(CommandGetTableTypes.class)) {
      getStreamTableTypes(context, ticket, listener);
    } else if (command.is(CommandGetSqlInfo.class)) {
      getStreamSqlInfo(FlightSqlUtils.unpackOrThrow(command, CommandGetSqlInfo.class), context, ticket, listener);
    } else if (command.is(CommandGetPrimaryKeys.class)) {
      getStreamPrimaryKeys(FlightSqlUtils.unpackOrThrow(command, CommandGetPrimaryKeys.class),
          context, ticket, listener);
    } else if (command.is(CommandGetForeignKeys.class)) {
      getStreamForeignKeys(FlightSqlUtils.unpackOrThrow(command, CommandGetForeignKeys.class),
          context, ticket, listener);
    } else {
      throw Status.INVALID_ARGUMENT.asRuntimeException();
    }
  }

  /**
   * Depending on the provided command, method either:
   * 1. Execute provided SQL query as an update statement, or
   * 2. Execute provided update SQL query prepared statement. In this case, parameters binding
   * is allowed, or
   * 3. Binds parameters to the provided prepared statement.
   *
   * @param context      Per-call context.
   * @param flightStream The data stream being uploaded.
   * @param ackStream    The data stream listener for update result acknowledgement.
   * @return a Runnable to process the stream.
   */
  @Override
  public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
    final Any command = FlightSqlUtils.parseOrThrow(flightStream.getDescriptor().getCommand());

    if (command.is(CommandStatementUpdate.class)) {
      return acceptPutStatement(
          FlightSqlUtils.unpackOrThrow(command, CommandStatementUpdate.class),
          context, flightStream, ackStream);
    } else if (command.is(CommandPreparedStatementUpdate.class)) {
      return acceptPutPreparedStatementUpdate(
          FlightSqlUtils.unpackOrThrow(command, CommandPreparedStatementUpdate.class),
          context, flightStream, ackStream);
    } else if (command.is(CommandPreparedStatementQuery.class)) {
      return acceptPutPreparedStatementQuery(
          FlightSqlUtils.unpackOrThrow(command, CommandPreparedStatementQuery.class),
          context, flightStream, ackStream);
    }

    throw Status.INVALID_ARGUMENT.asRuntimeException();
  }

  /**
   * Lists all available Flight SQL actions.
   *
   * @param context  Per-call context.
   * @param listener An interface for sending data back to the client.
   */
  @Override
  public void listActions(CallContext context, StreamListener<ActionType> listener) {
    FlightSqlUtils.FLIGHT_SQL_ACTIONS.forEach(listener::onNext);
    listener.onCompleted();
  }

  /**
   * Performs the requested Flight SQL action.
   *
   * @param context  Per-call context.
   * @param action   Client-supplied parameters.
   * @param listener A stream of responses.
   */
  @Override
  public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
    if (action.getType().equals(FlightSqlUtils.FLIGHT_SQL_CREATEPREPAREDSTATEMENT.getType())) {
      final ActionCreatePreparedStatementRequest request = FlightSqlUtils.unpackAndParseOrThrow(action.getBody(),
          ActionCreatePreparedStatementRequest.class);
      createPreparedStatement(request, context, listener);
    } else if (action.getType().equals(FlightSqlUtils.FLIGHT_SQL_CLOSEPREPAREDSTATEMENT.getType())) {
      final ActionClosePreparedStatementRequest request = FlightSqlUtils.unpackAndParseOrThrow(action.getBody(),
          ActionClosePreparedStatementRequest.class);
      closePreparedStatement(request, context, listener);
    }

    throw Status.INVALID_ARGUMENT.asRuntimeException();
  }

  /**
   * Creates a prepared statement on the server and returns a handle and metadata for in a
   * {@link org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementResult} object in a {@link Result}
   * object.
   *
   * @param request  The sql command to generate the prepared statement.
   * @param context  Per-call context.
   * @param listener A stream of responses.
   */
  public abstract void createPreparedStatement(ActionCreatePreparedStatementRequest request, CallContext context,
                                               StreamListener<Result> listener);

  /**
   * Closes a prepared statement on the server. No result is expected.
   *
   * @param request  The sql command to generate the prepared statement.
   * @param context  Per-call context.
   * @param listener A stream of responses.
   */
  public abstract void closePreparedStatement(ActionClosePreparedStatementRequest request, CallContext context,
                                              StreamListener<Result> listener);

  /**
   * Gets information about a particular SQL query based data stream.
   *
   * @param command    The sql command to generate the data stream.
   * @param context    Per-call context.
   * @param descriptor The descriptor identifying the data stream.
   * @return Metadata about the stream.
   */
  public abstract FlightInfo getFlightInfoStatement(CommandStatementQuery command, CallContext context,
                                                    FlightDescriptor descriptor);

  /**
   * Gets information about a particular prepared statement data stream.
   *
   * @param command    The prepared statement to generate the data stream.
   * @param context    Per-call context.
   * @param descriptor The descriptor identifying the data stream.
   * @return Metadata about the stream.
   */
  public abstract FlightInfo getFlightInfoPreparedStatement(CommandPreparedStatementQuery command,
                                                            CallContext context, FlightDescriptor descriptor);

  /**
   * Gets schema about a particular SQL query based data stream.
   *
   * @param command    The sql command to generate the data stream.
   * @param context    Per-call context.
   * @param descriptor The descriptor identifying the data stream.
   * @return Schema for the stream.
   */
  public abstract SchemaResult getSchemaStatement(CommandStatementQuery command, CallContext context,
                                                  FlightDescriptor descriptor);

  /**
   * Returns data for a SQL query based data stream.
   *
   * @param command  The sql command to generate the data stream.
   * @param context  Per-call context.
   * @param ticket   The application-defined ticket identifying this stream.
   * @param listener An interface for sending data back to the client.
   */
  public abstract void getStreamStatement(CommandStatementQuery command, CallContext context, Ticket ticket,
                                          ServerStreamListener listener);

  /**
   * Returns data for a particular prepared statement query instance.
   *
   * @param command  The prepared statement to generate the data stream.
   * @param context  Per-call context.
   * @param ticket   The application-defined ticket identifying this stream.
   * @param listener An interface for sending data back to the client.
   */
  public abstract void getStreamPreparedStatement(CommandPreparedStatementQuery command, CallContext context,
                                                  Ticket ticket, ServerStreamListener listener);

  /**
   * Accepts uploaded data for a particular SQL query based data stream.
   * <p>`PutResult`s must be in the form of a {@link DoPutUpdateResult}.
   *
   * @param command      The sql command to generate the data stream.
   * @param context      Per-call context.
   * @param flightStream The data stream being uploaded.
   * @param ackStream    The result data stream.
   * @return A runnable to process the stream.
   */
  public abstract Runnable acceptPutStatement(CommandStatementUpdate command, CallContext context,
                                              FlightStream flightStream, StreamListener<PutResult> ackStream);

  /**
   * Accepts uploaded data for a particular prepared statement data stream.
   * <p>`PutResult`s must be in the form of a {@link DoPutUpdateResult}.
   *
   * @param command      The prepared statement to generate the data stream.
   * @param context      Per-call context.
   * @param flightStream The data stream being uploaded.
   * @param ackStream    The result data stream.
   * @return A runnable to process the stream.
   */
  public abstract Runnable acceptPutPreparedStatementUpdate(CommandPreparedStatementUpdate command,
                                                            CallContext context, FlightStream flightStream,
                                                            StreamListener<PutResult> ackStream);

  /**
   * Accepts uploaded parameter values for a particular prepared statement query.
   *
   * @param command      The prepared statement the parameter values will bind to.
   * @param context      Per-call context.
   * @param flightStream The data stream being uploaded.
   * @param ackStream    The result data stream.
   * @return A runnable to process the stream.
   */
  public abstract Runnable acceptPutPreparedStatementQuery(CommandPreparedStatementQuery command,
                                                           CallContext context, FlightStream flightStream,
                                                           StreamListener<PutResult> ackStream);

  /**
   * Returns the SQL Info of the server by returning a
   * {@link CommandGetSqlInfo} in a {@link Result}.
   *
   * @param request    request filter parameters.
   * @param context    Per-call context.
   * @param descriptor The descriptor identifying the data stream.
   * @return Metadata about the stream.
   */
  public abstract FlightInfo getFlightInfoSqlInfo(CommandGetSqlInfo request, CallContext context,
                                                  FlightDescriptor descriptor);

  /**
   * Gets schema about the get SQL info data stream.
   *
   * @return Schema for the stream.
   */
  public SchemaResult getSchemaSqlInfo() {

    final List<Field> children = Arrays.asList(
        Field.nullable("string_value", MinorType.VARCHAR.getType()),
        Field.nullable("int_value", MinorType.INT.getType()),
        Field.nullable("bigint_value", MinorType.BIGINT.getType()),
        Field.nullable("int32_bitmask", MinorType.INT.getType()));

    List<Field> fields = Arrays.asList(
        Field.nullable("info_name", MinorType.VARCHAR.getType()),
        new Field("value",
            // dense_union<string_value: string, int_value: int32, bigint_value: int64, int32_bitmask: int32>
            new FieldType(false, new Union(UnionMode.Dense, new int[] {0, 1, 2, 3}), /*dictionary=*/null),
            children));

    return new SchemaResult(new Schema(fields));
  }

  /**
   * Returns data for SQL info based data stream.
   *
   * @param command  The command to generate the data stream.
   * @param context  Per-call context.
   * @param ticket   The application-defined ticket identifying this stream.
   * @param listener An interface for sending data back to the client.
   */
  public abstract void getStreamSqlInfo(CommandGetSqlInfo command, CallContext context, Ticket ticket,
                                        ServerStreamListener listener);

  /**
   * Returns the available catalogs by returning a stream of
   * {@link org.apache.arrow.flight.sql.impl.FlightSql.CommandGetCatalogs} objects in {@link Result} objects.
   *
   * @param request    request filter parameters.
   * @param context    Per-call context.
   * @param descriptor The descriptor identifying the data stream.
   * @return Metadata about the stream.
   */
  public abstract FlightInfo getFlightInfoCatalogs(CommandGetCatalogs request, CallContext context,
                                                   FlightDescriptor descriptor);

  /**
   * Gets schema about the get catalogs data stream.
   *
   * @return Schema for the stream.
   */
  public SchemaResult getSchemaCatalogs() {
    return new SchemaResult(GET_CATALOGS_SCHEMA);
  }

  /**
   * Returns data for catalogs based data stream.
   *
   * @param context  Per-call context.
   * @param ticket   The application-defined ticket identifying this stream.
   * @param listener An interface for sending data back to the client.
   */
  public abstract void getStreamCatalogs(CallContext context, Ticket ticket,
                                         ServerStreamListener listener);

  /**
   * Returns the available schemas by returning a stream of
   * {@link CommandGetSchemas} objects in {@link Result} objects.
   *
   * @param request    request filter parameters.
   * @param context    Per-call context.
   * @param descriptor The descriptor identifying the data stream.
   * @return Metadata about the stream.
   */
  public abstract FlightInfo getFlightInfoSchemas(CommandGetSchemas request, CallContext context,
                                                  FlightDescriptor descriptor);

  /**
   * Gets schema about the get schemas data stream.
   *
   * @return Schema for the stream.
   */
  public SchemaResult getSchemaSchemas() {
    return new SchemaResult(GET_SCHEMAS_SCHEMA);
  }

  /**
   * Returns data for schemas based data stream.
   *
   * @param command  The command to generate the data stream.
   * @param context  Per-call context.
   * @param ticket   The application-defined ticket identifying this stream.
   * @param listener An interface for sending data back to the client.
   */
  public abstract void getStreamSchemas(CommandGetSchemas command, CallContext context, Ticket ticket,
                                        ServerStreamListener listener);

  /**
   * Returns the available tables by returning a stream of
   * {@link CommandGetTables} objects in {@link Result} objects.
   *
   * @param request    request filter parameters.
   * @param context    Per-call context.
   * @param descriptor The descriptor identifying the data stream.
   * @return Metadata about the stream.
   */
  public abstract FlightInfo getFlightInfoTables(CommandGetTables request, CallContext context,
                                                 FlightDescriptor descriptor);

  /**
   * Gets schema about the get tables data stream.
   *
   * @return Schema for the stream.
   */
  public SchemaResult getSchemaTables() {
    return new SchemaResult(GET_TABLES_SCHEMA);
  }

  /**
   * Returns data for tables based data stream.
   *
   * @param command  The command to generate the data stream.
   * @param context  Per-call context.
   * @param ticket   The application-defined ticket identifying this stream.
   * @param listener An interface for sending data back to the client.
   */
  public abstract void getStreamTables(CommandGetTables command, CallContext context, Ticket ticket,
                                       ServerStreamListener listener);

  /**
   * Returns the available table types by returning a stream of
   * {@link CommandGetTableTypes} objects in {@link Result} objects.
   *
   * @param context    Per-call context.
   * @param descriptor The descriptor identifying the data stream.
   * @return Metadata about the stream.
   */
  public abstract FlightInfo getFlightInfoTableTypes(CommandGetTableTypes request, CallContext context,
                                                     FlightDescriptor descriptor);

  /**
   * Gets schema about the get table types data stream.
   *
   * @return Schema for the stream.
   */
  public SchemaResult getSchemaTableTypes() {
    return new SchemaResult(GET_TABLE_TYPES_SCHEMA);
  }

  /**
   * Returns data for table types based data stream.
   *
   * @param context  Per-call context.
   * @param ticket   The application-defined ticket identifying this stream.
   * @param listener An interface for sending data back to the client.
   */
  public abstract void getStreamTableTypes(CallContext context, Ticket ticket, ServerStreamListener listener);

  /**
   * Returns the available primary keys by returning a stream of
   * {@link CommandGetPrimaryKeys} objects in {@link Result} objects.
   *
   * @param request    request filter parameters.
   * @param context    Per-call context.
   * @param descriptor The descriptor identifying the data stream.
   * @return Metadata about the stream.
   */
  public abstract FlightInfo getFlightInfoPrimaryKeys(CommandGetPrimaryKeys request, CallContext context,
                                                      FlightDescriptor descriptor);

  /**
   * Gets schema about the get primary keys data stream.
   *
   * @return Schema for the stream.
   */
  public SchemaResult getSchemaPrimaryKeys() {
    final List<Field> fields = Arrays.asList(
        Field.nullable("catalog_name", MinorType.VARCHAR.getType()),
        Field.nullable("schema_name", MinorType.VARCHAR.getType()),
        Field.nullable("table_name", MinorType.VARCHAR.getType()),
        Field.nullable("column_name", MinorType.VARCHAR.getType()),
        Field.nullable("key_sequence", MinorType.INT.getType()),
        Field.nullable("key_name", MinorType.VARCHAR.getType()));

    return new SchemaResult(new Schema(fields));
  }

  /**
   * Returns data for primary keys based data stream.
   *
   * @param command  The command to generate the data stream.
   * @param context  Per-call context.
   * @param ticket   The application-defined ticket identifying this stream.
   * @param listener An interface for sending data back to the client.
   */
  public abstract void getStreamPrimaryKeys(CommandGetPrimaryKeys command, CallContext context, Ticket ticket,
                                            ServerStreamListener listener);

  /**
   * Returns the available primary keys by returning a stream of
   * {@link CommandGetForeignKeys} objects in {@link Result} objects.
   *
   * @param request    request filter parameters.
   * @param context    Per-call context.
   * @param descriptor The descriptor identifying the data stream.
   * @return Metadata about the stream.
   */
  public abstract FlightInfo getFlightInfoForeignKeys(CommandGetForeignKeys request, CallContext context,
                                                      FlightDescriptor descriptor);

  /**
   * Gets schema about the get foreign keys data stream.
   *
   * @return Schema for the stream.
   */
  public SchemaResult getSchemaForeignKeys() {
    final List<Field> fields = Arrays.asList(
        Field.nullable("pk_catalog_name", MinorType.VARCHAR.getType()),
        Field.nullable("pk_schema_name", MinorType.VARCHAR.getType()),
        Field.nullable("pk_table_name", MinorType.VARCHAR.getType()),
        Field.nullable("pk_column_name", MinorType.VARCHAR.getType()),
        Field.nullable("fk_catalog_name", MinorType.VARCHAR.getType()),
        Field.nullable("fk_schema_name", MinorType.VARCHAR.getType()),
        Field.nullable("fk_table_name", MinorType.VARCHAR.getType()),
        Field.nullable("fk_column_name", MinorType.VARCHAR.getType()),
        Field.nullable("key_sequence", MinorType.INT.getType()),
        Field.nullable("fk_key_name", MinorType.VARCHAR.getType()),
        Field.nullable("pk_key_name", MinorType.VARCHAR.getType()),
        Field.nullable("update_rule", MinorType.INT.getType()),
        Field.nullable("delete_rule", MinorType.INT.getType()));

    return new SchemaResult(new Schema(fields));
  }

  /**
   * Returns data for foreign keys based data stream.
   *
   * @param command  The command to generate the data stream.
   * @param context  Per-call context.
   * @param ticket   The application-defined ticket identifying this stream.
   * @param listener An interface for sending data back to the client.
   */
  public abstract void getStreamForeignKeys(CommandGetForeignKeys command, CallContext context, Ticket ticket,
                                            ServerStreamListener listener);
}
