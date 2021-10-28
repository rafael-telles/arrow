// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <arrow/flight/client.h>
#include <arrow/flight/flight-sql/FlightSql.pb.h>
#include <arrow/flight/flight-sql/api.h>
#include <arrow/testing/gtest_util.h>
#include <gmock/gmock.h>
#include <google/protobuf/any.pb.h>
#include <gtest/gtest.h>

#include <utility>

namespace pb = arrow::flight::protocol;
using ::testing::_;
using ::testing::Ref;

namespace arrow {
namespace flight {
namespace sql {

namespace internal {
class FlightClientImpl {
 public:
  ~FlightClientImpl() = default;

  MOCK_METHOD(Status, GetFlightInfo,
              (const FlightCallOptions&, const FlightDescriptor&,
               std::unique_ptr<FlightInfo>*));
  MOCK_METHOD(Status, DoGet,
              (const FlightCallOptions& options, const Ticket& ticket,
               std::unique_ptr<FlightStreamReader>* stream));
  MOCK_METHOD(Status, DoPut,
              (const FlightCallOptions&, const FlightDescriptor&,
               const std::shared_ptr<Schema>& schema,
               std::unique_ptr<FlightStreamWriter>*,
               std::unique_ptr<FlightMetadataReader>*));
  MOCK_METHOD(Status, DoAction,
              (const FlightCallOptions& options, const Action& action,
               std::unique_ptr<ResultStream>* results));
};

Status FlightClientImpl_GetFlightInfo(FlightClientImpl* client,
                                      const FlightCallOptions& options,
                                      const FlightDescriptor& descriptor,
                                      std::unique_ptr<FlightInfo>* info) {
  return client->GetFlightInfo(options, descriptor, info);
}

Status FlightClientImpl_DoPut(FlightClientImpl* client, const FlightCallOptions& options,
                              const FlightDescriptor& descriptor,
                              const std::shared_ptr<Schema>& schema,
                              std::unique_ptr<FlightStreamWriter>* stream,
                              std::unique_ptr<FlightMetadataReader>* reader) {
  return client->DoPut(options, descriptor, schema, stream, reader);
}

Status FlightClientImpl_DoGet(FlightClientImpl* client, const FlightCallOptions& options,
                              const Ticket& ticket,
                              std::unique_ptr<FlightStreamReader>* stream) {
  return client->DoGet(options, ticket, stream);
}

Status FlightClientImpl_DoAction(FlightClientImpl* client,
                                 const FlightCallOptions& options, const Action& action,
                                 std::unique_ptr<ResultStream>* results) {
  return client->DoAction(options, action, results);
}

}  // namespace internal

class FlightMetadataReaderMock : public FlightMetadataReader {
 public:
  std::shared_ptr<Buffer>* buffer;

  explicit FlightMetadataReaderMock(std::shared_ptr<Buffer>* buffer) {
    this->buffer = buffer;
  }

  Status ReadMetadata(std::shared_ptr<Buffer>* out) override {
    *out = *buffer;
    return Status::OK();
  }
};

class FlightStreamWriterMock : public FlightStreamWriter {
 public:
  FlightStreamWriterMock() = default;

  Status DoneWriting() override { return Status::OK(); }

  Status WriteMetadata(std::shared_ptr<Buffer> app_metadata) override {
    return Status::OK();
  }

  Status Begin(const std::shared_ptr<Schema>& schema,
               const ipc::IpcWriteOptions& options) override {
    return Status::OK();
  }

  Status Begin(const std::shared_ptr<Schema>& schema) override {
    return MetadataRecordBatchWriter::Begin(schema);
  }

  ipc::WriteStats stats() const override { return ipc::WriteStats(); }

  Status WriteWithMetadata(const RecordBatch& batch,
                           std::shared_ptr<Buffer> app_metadata) override {
    return Status::OK();
  }

  Status Close() override { return Status::OK(); }

  Status WriteRecordBatch(const RecordBatch& batch) override { return Status::OK(); }
};

FlightDescriptor getDescriptor(google::protobuf::Message& command) {
  google::protobuf::Any any;
  any.PackFrom(command);

  const std::string& string = any.SerializeAsString();
  return FlightDescriptor::Command(string);
}

TEST(TestFlightSqlClient, TestGetCatalogs) {
  auto client_mock = std::make_shared<internal::FlightClientImpl>();
  FlightSqlClient sql_client(client_mock);
  FlightCallOptions call_options;

  pb::sql::CommandGetCatalogs command;
  FlightDescriptor descriptor = getDescriptor(command);

  EXPECT_CALL(*client_mock, GetFlightInfo(Ref(call_options), descriptor, _));

  ASSERT_OK(sql_client.GetCatalogs(call_options));
}

TEST(TestFlightSqlClient, TestGetSchemas) {
  auto client_mock = std::make_shared<internal::FlightClientImpl>();
  FlightSqlClient sql_client(client_mock);
  FlightCallOptions call_options;

  std::string schema_filter_pattern = "schema_filter_pattern";
  std::string catalog = "catalog";

  pb::sql::CommandGetSchemas command;
  command.set_catalog(catalog);
  command.set_schema_filter_pattern(schema_filter_pattern);
  FlightDescriptor descriptor = getDescriptor(command);

  EXPECT_CALL(*client_mock, GetFlightInfo(Ref(call_options), descriptor, _));

  ASSERT_OK(sql_client.GetSchemas(call_options, &catalog, &schema_filter_pattern));
}

TEST(TestFlightSqlClient, TestGetTables) {
  auto client_mock = std::make_shared<internal::FlightClientImpl>();
  FlightSqlClient sql_client(client_mock);
  FlightCallOptions call_options;

  std::string catalog = "catalog";
  std::string schema_filter_pattern = "schema_filter_pattern";
  std::string table_name_filter_pattern = "table_name_filter_pattern";
  bool include_schema = true;
  std::vector<std::string> table_types = {"type1", "type2"};

  pb::sql::CommandGetTables command;
  command.set_catalog(catalog);
  command.set_schema_filter_pattern(schema_filter_pattern);
  command.set_table_name_filter_pattern(table_name_filter_pattern);
  command.set_include_schema(include_schema);
  for (const std::string& table_type : table_types) {
    command.add_table_types(table_type);
  }
  FlightDescriptor descriptor = getDescriptor(command);

  EXPECT_CALL(*client_mock, GetFlightInfo(Ref(call_options), descriptor, _));

  ASSERT_OK(sql_client.GetTables(call_options, &catalog, &schema_filter_pattern,
                                 &table_name_filter_pattern, include_schema,
                                 table_types));
}

TEST(TestFlightSqlClient, TestGetTableTypes) {
  auto client_mock = std::make_shared<internal::FlightClientImpl>();
  FlightSqlClient sql_client(client_mock);
  FlightCallOptions call_options;

  pb::sql::CommandGetTableTypes command;
  FlightDescriptor descriptor = getDescriptor(command);

  EXPECT_CALL(*client_mock, GetFlightInfo(Ref(call_options), descriptor, _));

  ASSERT_OK(sql_client.GetTableTypes(call_options));
}

TEST(TestFlightSqlClient, TestGetExported) {
  auto client_mock = std::make_shared<internal::FlightClientImpl>();
  FlightSqlClient sql_client(client_mock);
  FlightCallOptions call_options;

  std::string catalog = "catalog";
  std::string schema = "schema";
  std::string table = "table";

  pb::sql::CommandGetExportedKeys command;
  command.set_catalog(catalog);
  command.set_schema(schema);
  command.set_table(table);
  FlightDescriptor descriptor = getDescriptor(command);

  EXPECT_CALL(*client_mock, GetFlightInfo(Ref(call_options), descriptor, _));

  ASSERT_OK(sql_client.GetExportedKeys(call_options, &catalog, &schema, table));
}

TEST(TestFlightSqlClient, TestGetImported) {
  auto client_mock = std::make_shared<internal::FlightClientImpl>();
  FlightSqlClient sql_client(client_mock);
  FlightCallOptions call_options;

  std::string catalog = "catalog";
  std::string schema = "schema";
  std::string table = "table";

  pb::sql::CommandGetImportedKeys command;
  command.set_catalog(catalog);
  command.set_schema(schema);
  command.set_table(table);
  FlightDescriptor descriptor = getDescriptor(command);

  EXPECT_CALL(*client_mock, GetFlightInfo(Ref(call_options), descriptor, _));

  ASSERT_OK(sql_client.GetImportedKeys(call_options, &catalog, &schema, table));
}

TEST(TestFlightSqlClient, TestGetPrimary) {
  auto client_mock = std::make_shared<internal::FlightClientImpl>();
  FlightSqlClient sql_client(client_mock);
  FlightCallOptions call_options;

  std::string catalog = "catalog";
  std::string schema = "schema";
  std::string table = "table";

  pb::sql::CommandGetPrimaryKeys command;
  command.set_catalog(catalog);
  command.set_schema(schema);
  command.set_table(table);
  FlightDescriptor descriptor = getDescriptor(command);

  EXPECT_CALL(*client_mock, GetFlightInfo(Ref(call_options), descriptor, _));

  ASSERT_OK(sql_client.GetPrimaryKeys(call_options, &catalog, &schema, table));
}

TEST(TestFlightSqlClient, TestGetCrossReference) {
  auto client_mock = std::make_shared<internal::FlightClientImpl>();
  FlightSqlClient sql_client(client_mock);
  FlightCallOptions call_options;

  std::string pk_catalog = "pk_catalog";
  std::string pk_schema = "pk_schema";
  std::string pk_table = "pk_table";
  std::string fk_catalog = "fk_catalog";
  std::string fk_schema = "fk_schema";
  std::string fk_table = "fk_table";

  pb::sql::CommandGetCrossReference command;
  command.set_pk_catalog(pk_catalog);
  command.set_pk_schema(pk_schema);
  command.set_pk_table(pk_table);
  command.set_fk_catalog(fk_catalog);
  command.set_fk_schema(fk_schema);
  command.set_fk_table(fk_table);
  FlightDescriptor descriptor = getDescriptor(command);

  EXPECT_CALL(*client_mock, GetFlightInfo(Ref(call_options), descriptor, _));

  ASSERT_OK(sql_client.GetCrossReference(call_options, &pk_catalog, &pk_schema, pk_table,
                                         &fk_catalog, &fk_schema, fk_table));
}

TEST(TestFlightSqlClient, TestExecute) {
  auto client_mock = std::make_shared<internal::FlightClientImpl>();
  FlightSqlClient sql_client(client_mock);
  FlightCallOptions call_options;

  std::string query = "query";

  pb::sql::CommandStatementQuery command;
  command.set_query(query);
  FlightDescriptor descriptor = getDescriptor(command);

  EXPECT_CALL(*client_mock, GetFlightInfo(Ref(call_options), descriptor, _));

  ASSERT_OK(sql_client.Execute(call_options, query));
}

TEST(TestFlightSqlClient, TestPreparedStatementExecute) {
  auto client_mock = std::make_shared<internal::FlightClientImpl>();
  FlightSqlClient sql_client(client_mock);
  FlightCallOptions call_options;

  const std::string query = "query";

  ON_CALL(*client_mock, DoAction)
      .WillByDefault([](const FlightCallOptions& options, const Action& action,
                        std::unique_ptr<ResultStream>* results) {
        google::protobuf::Any command;

        pb::sql::ActionCreatePreparedStatementResult prepared_statement_result;

        prepared_statement_result.set_prepared_statement_handle("query");

        command.PackFrom(prepared_statement_result);

        *results = std::unique_ptr<ResultStream>(new SimpleResultStream(
            {Result{Buffer::FromString(command.SerializeAsString())}}));

        return Status::OK();
      });

  EXPECT_CALL(*client_mock, DoAction(_, _, _)).Times(2);

  ASSERT_OK_AND_ASSIGN(auto prepared_statement, sql_client.Prepare(call_options, query));

  EXPECT_CALL(*client_mock, GetFlightInfo(_, _, _));

  ASSERT_OK(prepared_statement->Execute());
}

TEST(TestFlightSqlClient, TestPreparedStatementExecuteParameterBinding) {
  auto client_mock = std::make_shared<internal::FlightClientImpl>();
  FlightSqlClient sql_client(client_mock);
  FlightCallOptions call_options;

  const std::string query = "query";

  ON_CALL(*client_mock, DoAction)
      .WillByDefault([](const FlightCallOptions& options, const Action& action,
                        std::unique_ptr<ResultStream>* results) {
        google::protobuf::Any command;

        pb::sql::ActionCreatePreparedStatementResult prepared_statement_result;

        prepared_statement_result.set_prepared_statement_handle("query");

        auto schema = arrow::schema({arrow::field("id", int64())});

        std::shared_ptr<Buffer> schema_buffer;
        const arrow::Result<std::shared_ptr<Buffer>>& result =
            arrow::ipc::SerializeSchema(*schema);

        ARROW_ASSIGN_OR_RAISE(schema_buffer, result);

        prepared_statement_result.set_parameter_schema(schema_buffer->ToString());

        command.PackFrom(prepared_statement_result);

        *results = std::unique_ptr<ResultStream>(new SimpleResultStream(
            {Result{Buffer::FromString(command.SerializeAsString())}}));

        return Status::OK();
      });

  std::shared_ptr<Buffer> buffer_ptr;
  ON_CALL(*client_mock, DoPut)
      .WillByDefault([&buffer_ptr](const FlightCallOptions& options,
                                   const FlightDescriptor& descriptor1,
                                   const std::shared_ptr<Schema>& schema,
                                   std::unique_ptr<FlightStreamWriter>* writer,
                                   std::unique_ptr<FlightMetadataReader>* reader) {
        writer->reset(new FlightStreamWriterMock());
        reader->reset(new FlightMetadataReaderMock(&buffer_ptr));

        return Status::OK();
      });

  EXPECT_CALL(*client_mock, DoAction(_, _, _)).Times(2);
  EXPECT_CALL(*client_mock, DoPut(_, _, _, _, _));

  ASSERT_OK_AND_ASSIGN(auto prepared_statement, sql_client.Prepare(call_options, query));

  ASSERT_OK_AND_ASSIGN(auto parameter_schema, prepared_statement->GetParameterSchema());

  arrow::Int64Builder int_builder;
  ASSERT_OK(int_builder.Append(1));
  std::shared_ptr<arrow::Array> int_array;
  ASSERT_OK(int_builder.Finish(&int_array));
  std::shared_ptr<arrow::RecordBatch> result;
  result = arrow::RecordBatch::Make(parameter_schema, 1, {int_array});
  ASSERT_OK(prepared_statement->SetParameters(result));

  EXPECT_CALL(*client_mock, GetFlightInfo(_, _, _));

  ASSERT_OK(prepared_statement->Execute());
}

TEST(TestFlightSqlClient, TestExecuteUpdate) {
  auto client_mock = std::make_shared<internal::FlightClientImpl>();
  FlightSqlClient sql_client(client_mock);
  FlightCallOptions call_options;

  std::string query = "query";

  pb::sql::CommandStatementUpdate command;

  command.set_query(query);

  google::protobuf::Any any;
  any.PackFrom(command);

  const FlightDescriptor& descriptor = FlightDescriptor::Command(any.SerializeAsString());

  pb::sql::DoPutUpdateResult doPutUpdateResult;
  doPutUpdateResult.set_record_count(100);
  const std::string& string = doPutUpdateResult.SerializeAsString();

  auto buffer_ptr = std::make_shared<Buffer>(
      reinterpret_cast<const uint8_t*>(string.data()), doPutUpdateResult.ByteSizeLong());

  ON_CALL(*client_mock, DoPut)
      .WillByDefault([&buffer_ptr](const FlightCallOptions& options,
                                   const FlightDescriptor& descriptor1,
                                   const std::shared_ptr<Schema>& schema,
                                   std::unique_ptr<FlightStreamWriter>* writer,
                                   std::unique_ptr<FlightMetadataReader>* reader) {
        reader->reset(new FlightMetadataReaderMock(&buffer_ptr));

        return Status::OK();
      });

  std::unique_ptr<FlightInfo> flight_info;
  std::unique_ptr<FlightStreamWriter> writer;
  std::unique_ptr<FlightMetadataReader> reader;
  EXPECT_CALL(*client_mock, DoPut(Ref(call_options), descriptor, _, _, _));

  ASSERT_OK_AND_ASSIGN(auto num_rows, sql_client.ExecuteUpdate(call_options, query));

  ASSERT_EQ(num_rows, 100);
}

TEST(TestFlightSqlClient, TestGetSqlInfo) {
  auto client_mock = std::make_shared<internal::FlightClientImpl>();
  FlightSqlClient sql_client(client_mock);

  std::vector<pb::sql::SqlInfo> sql_info{
      pb::sql::SqlInfo::FLIGHT_SQL_SERVER_NAME,
      pb::sql::SqlInfo::FLIGHT_SQL_SERVER_VERSION,
      pb::sql::SqlInfo::FLIGHT_SQL_SERVER_ARROW_VERSION};
  pb::sql::CommandGetSqlInfo command;

  for (const auto& info : sql_info) command.add_info(info);
  google::protobuf::Any any;
  any.PackFrom(command);
  const FlightDescriptor& descriptor = FlightDescriptor::Command(any.SerializeAsString());

  FlightCallOptions call_options;
  EXPECT_CALL(*client_mock, GetFlightInfo(Ref(call_options), descriptor, _));
  ASSERT_OK(sql_client.GetSqlInfo(call_options, sql_info));
}

template <class Func>
inline void AssertTestPreparedStatementExecuteUpdateOk(
    Func func, const std::shared_ptr<Schema>* schema) {
  auto client_mock = std::make_shared<internal::FlightClientImpl>();
  FlightSqlClient sql_client(client_mock);

  const std::string query = "SELECT * FROM IRRELEVANT";
  const FlightCallOptions call_options;
  int64_t expected_rows = 100L;
  pb::sql::DoPutUpdateResult result;
  result.set_record_count(expected_rows);

  ON_CALL(*client_mock, DoAction)
      .WillByDefault([&query, &schema](const FlightCallOptions& options,
                                       const Action& action,
                                       std::unique_ptr<ResultStream>* results) {
        google::protobuf::Any command;
        pb::sql::ActionCreatePreparedStatementResult prepared_statement_result;

        prepared_statement_result.set_prepared_statement_handle(query);

        if (schema != NULLPTR) {
          std::shared_ptr<Buffer> schema_buffer;
          const arrow::Result<std::shared_ptr<Buffer>>& result =
              arrow::ipc::SerializeSchema(**schema);

          ARROW_ASSIGN_OR_RAISE(schema_buffer, result);
          prepared_statement_result.set_parameter_schema(schema_buffer->ToString());
        }

        command.PackFrom(prepared_statement_result);
        *results = std::unique_ptr<ResultStream>(new SimpleResultStream(
            {Result{Buffer::FromString(command.SerializeAsString())}}));

        return Status::OK();
      });
  EXPECT_CALL(*client_mock, DoAction(_, _, _)).Times(2);

  auto buffer = Buffer::FromString(result.SerializeAsString());
  ON_CALL(*client_mock, DoPut)
      .WillByDefault([&buffer](const FlightCallOptions& options,
                               const FlightDescriptor& descriptor1,
                               const std::shared_ptr<Schema>& schema,
                               std::unique_ptr<FlightStreamWriter>* writer,
                               std::unique_ptr<FlightMetadataReader>* reader) {
        reader->reset(new FlightMetadataReaderMock(&buffer));
        writer->reset(new FlightStreamWriterMock());
        return Status::OK();
      });
  if (schema == NULLPTR) {
    EXPECT_CALL(*client_mock, DoPut(_, _, _, _, _));
  } else {
    EXPECT_CALL(*client_mock, DoPut(_, _, *schema, _, _));
  }

  ASSERT_OK_AND_ASSIGN(auto prepared_statement, sql_client.Prepare(call_options, query));
  func(prepared_statement, *client_mock, schema, expected_rows);
  ASSERT_OK_AND_ASSIGN(auto rows, prepared_statement->ExecuteUpdate());
  ASSERT_EQ(expected_rows, rows);
}

TEST(TestFlightSqlClient, TestPreparedStatementExecuteUpdateNoParameterBinding) {
  AssertTestPreparedStatementExecuteUpdateOk(
      [](const std::shared_ptr<FlightSqlClient::PreparedStatement>& prepared_statement,
         internal::FlightClientImpl& client_mock, const std::shared_ptr<Schema>* schema,
         const int64_t& row_count) {},
      NULLPTR);
}

TEST(TestFlightSqlClient, TestPreparedStatementExecuteUpdateWithParameterBinding) {
  const auto schema = arrow::schema(
      {arrow::field("field0", arrow::utf8()), arrow::field("field1", arrow::uint8())});
  AssertTestPreparedStatementExecuteUpdateOk(
      [](const std::shared_ptr<FlightSqlClient::PreparedStatement>& prepared_statement,
         internal::FlightClientImpl& client_mock, const std::shared_ptr<Schema>* schema,
         const int64_t& row_count) {
        std::shared_ptr<Array> string_array;
        std::shared_ptr<Array> uint8_array;
        const std::vector<std::string> string_data{"Lorem", "Ipsum", "Foo", "Bar", "Baz"};
        const std::vector<uint8_t> uint8_data{0, 10, 15, 20, 25};
        ArrayFromVector<StringType, std::string>(string_data, &string_array);
        ArrayFromVector<UInt8Type, uint8_t>(uint8_data, &uint8_array);
        std::shared_ptr<RecordBatch> recordBatch =
            RecordBatch::Make(*schema, row_count, {string_array, uint8_array});
        ASSERT_OK(prepared_statement->SetParameters(recordBatch));
      },
      &schema);
}

}  // namespace sql
}  // namespace flight
}  // namespace arrow
