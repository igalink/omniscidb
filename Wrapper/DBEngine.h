/*
 * Copyright 2020 OmniSci, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <type_traits>
#include <vector>
#include "QueryEngine/ResultSet.h"

namespace EmbeddedDatabase {

class Row {
 public:
  Row();
  Row(const std::vector<TargetValue>& row);
  int64_t getIntScalarTargetValue(size_t col_idx);
  float getFloatScalarTargetValue(size_t col_idx);
  double getDoubleScalarTargetValue(size_t col_idx);
  std::string getStrScalarTargetValue(size_t col_idx);

 private:
  std::vector<TargetValue> m_row;
};

class Cursor {
 public:
  ExecutorDeviceType getDeviceType();
  Row getNextRow(const bool translate_strings, const bool decimal_to_double);
  OneIntegerColumnRow getOneColRow(const size_t index);
  size_t colCount();
  size_t rowCount(const bool force_parallel);
  void setCachedRowCount(const size_t row_count);
  size_t entryCount();
  bool definitelyHasNoRows();
  int8_t* getDeviceEstimatorBuffer();
  int8_t* getHostEstimatorBuffer();
  void setQueueTime(const int64_t queue_time);
  int64_t getQueueTime();
  int64_t getRenderTime();
  bool isTruncated();
  bool isExplain();
  bool isGeoColOnGpu(const size_t col_idx);
  int getDeviceId();
  const bool isPermutationBufferEmpty();
  size_t getLimit();

  // int getColType(uint32_t col_num);
};

class DBEngine {
 public:
  void reset();
  void executeDDL(std::string query);
  Cursor* executeDML(std::string query);
  static DBEngine* create(std::string path);

 protected:
  DBEngine() {}
};
}  // namespace EmbeddedDatabase
