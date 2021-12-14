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

#include "arrow/flight/sql/column_metadata.h"

const char* ColumnMetadata::CATALOG_NAME = "CATALOG_NAME";
const char* ColumnMetadata::SCHEMA_NAME = "SCHEMA_NAME";
const char* ColumnMetadata::TABLE_NAME = "TABLE_NAME";
const char* ColumnMetadata::PRECISION = "PRECISION";
const char* ColumnMetadata::SCALE = "SCALE";
const char* ColumnMetadata::IS_AUTO_INCREMENT = "IS_AUTO_INCREMENT";
const char* ColumnMetadata::IS_CASE_SENSITIVE = "IS_CASE_SENSITIVE";
const char* ColumnMetadata::IS_READ_ONLY = "IS_READ_ONLY";
const char* ColumnMetadata::IS_SEARCHABLE = "IS_SEARCHABLE";
const char* ColumnMetadata::BOOLEAN_TRUE_STR = "YES";
const char* ColumnMetadata::BOOLEAN_FALSE_STR = "NO";

ColumnMetadata::ColumnMetadata() : metadata_map_(
  std::make_shared<arrow::KeyValueMetadata>()) {
}

arrow::Result<std::string> ColumnMetadata::GetCatalogName() {
  return metadata_map_->Get(CATALOG_NAME);
}

arrow::Result<std::string> ColumnMetadata::GetSchemaName() {
  return metadata_map_->Get(SCHEMA_NAME);
}

arrow::Result<std::string> ColumnMetadata::GetTableName() {
  return metadata_map_->Get(TABLE_NAME);
}

arrow::Result<std::string> ColumnMetadata::GetPrecision() {
  return metadata_map_->Get(PRECISION);
}

arrow::Result<std::string> ColumnMetadata::GetScale() {
  return metadata_map_->Get(SCALE);
}

arrow::Result<std::string> ColumnMetadata::GetIsAutoIncrement() {
  return metadata_map_->Get(IS_AUTO_INCREMENT);
}

arrow::Result<std::string> ColumnMetadata::GetIsCaseSensitive() {
  return metadata_map_->Get(IS_CASE_SENSITIVE);
}

arrow::Result<std::string> ColumnMetadata::GetIsReadOnly() {
  return metadata_map_->Get(IS_READ_ONLY);
}

arrow::Result<std::string> ColumnMetadata::GetIsSearchable() {
  return metadata_map_->Get(IS_SEARCHABLE);
}

ColumnMetadataBuilder ColumnMetadata::Create() {
  const ColumnMetadataBuilder &builder = ColumnMetadataBuilder{};
  return builder;
}

std::shared_ptr<arrow::KeyValueMetadata> ColumnMetadata::GetMetadataMap() const {
  return metadata_map_;
}

ColumnMetadataBuilder &ColumnMetadataBuilder::CatalogName(std::string &catalog_name) {
  column_metadata_.GetMetadataMap()->Append(ColumnMetadata::CATALOG_NAME, catalog_name);
  return *this;
}


ColumnMetadataBuilder &ColumnMetadataBuilder::SchemaName(std::string &schema_name) {
  column_metadata_.GetMetadataMap()->Append(ColumnMetadata::SCHEMA_NAME, schema_name);
  return *this;
}

ColumnMetadataBuilder &ColumnMetadataBuilder::TableName(std::string &table_name) {
  column_metadata_.GetMetadataMap()->Append(ColumnMetadata::TABLE_NAME, table_name);
  return *this;
}

ColumnMetadataBuilder &ColumnMetadataBuilder::Precision(int32_t precision) {
  column_metadata_.GetMetadataMap()->Append(
    ColumnMetadata::PRECISION, std::to_string(precision));
  return *this;
}

ColumnMetadataBuilder &ColumnMetadataBuilder::Scale(int32_t scale) {
  column_metadata_.GetMetadataMap()->Append(
    ColumnMetadata::SCALE, std::to_string(scale));
  return *this;
}

ColumnMetadataBuilder &
ColumnMetadataBuilder::IsAutoIncrement(bool is_auto_increment) {
  column_metadata_.GetMetadataMap()->Append(ColumnMetadata::IS_AUTO_INCREMENT,
                                            BooleanToString(is_auto_increment));
  return *this;
}

ColumnMetadataBuilder &
ColumnMetadataBuilder::IsCaseSensitive(bool is_case_sensitive) {
  column_metadata_.GetMetadataMap()->Append(ColumnMetadata::IS_CASE_SENSITIVE,
                                            BooleanToString(is_case_sensitive));
  return *this;
}

ColumnMetadataBuilder &ColumnMetadataBuilder::IsReadOnly(bool is_read_only) {
  column_metadata_.GetMetadataMap()->Append(ColumnMetadata::IS_READ_ONLY,
                                            BooleanToString(is_read_only));
  return *this;
}

ColumnMetadataBuilder &ColumnMetadataBuilder::IsSearchable(bool is_searchable) {
  column_metadata_.GetMetadataMap()->Append(ColumnMetadata::IS_SEARCHABLE,
                                            BooleanToString(is_searchable));
  return *this;
}

ColumnMetadataBuilder::ColumnMetadataBuilder() : column_metadata_(
  ColumnMetadata()) {
}

std::string ColumnMetadataBuilder::BooleanToString(bool boolean_value) {
  return boolean_value ? ColumnMetadata::BOOLEAN_TRUE_STR :
    ColumnMetadata::BOOLEAN_FALSE_STR;
}

