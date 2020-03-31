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

#include "DBEngine.h"
#include <boost/filesystem.hpp>
#include <boost/variant.hpp>
#include <iostream>
#include "Catalog/Catalog.h"
#include "QueryRunner/QueryRunner.h"
#include "Shared/Logger.h"
#include "Shared/mapdpath.h"
#include "Shared/sqltypes.h"

namespace EmbeddedDatabase {

// enum class ColumnType : uint32_t { Unknown, Integer, Double, Float, String, Array };

/**
 * Cursor internal implementation
 */
class CursorImpl : public Cursor {
 public:
  CursorImpl(std::shared_ptr<ResultSet> result_set,
             std::shared_ptr<Data_Namespace::DataMgr> data_mgr)
      : m_result_set(result_set), m_data_mgr(data_mgr) {}

  ExecutorDeviceType getDeviceType() { return m_result_set->getDeviceType(); }

  Row getNextRow(const bool translate_strings, const bool decimal_to_double) {
    const auto row = m_result_set->getNextRow(translate_strings, decimal_to_double);
    if (row.empty()) {
      return Row{};
    }
    return Row(row);
  }

  OneIntegerColumnRow getOneColRow(const size_t index) {
    return m_result_set->getOneColRow(index);
  }

  size_t colCount() { return m_result_set->colCount(); }

  size_t rowCount(const bool force_parallel) {
    return m_result_set->rowCount(force_parallel);
  }

  void setCachedRowCount(const size_t row_count) {
    return m_result_set->setCachedRowCount(row_count);
  }

  size_t entryCount() { return m_result_set->entryCount(); }

  bool definitelyHasNoRows() { return m_result_set->definitelyHasNoRows(); }

  int8_t* getDeviceEstimatorBuffer() { return m_result_set->getDeviceEstimatorBuffer(); }

  int8_t* getHostEstimatorBuffer() { return m_result_set->getHostEstimatorBuffer(); }

  void setQueueTime(const int64_t queue_time) {
    return m_result_set->setQueueTime(queue_time);
  }

  int64_t getQueueTime() { return m_result_set->getQueueTime(); }

  int64_t getRenderTime() { return m_result_set->getRenderTime(); }

  bool isTruncated() { return m_result_set->isTruncated(); }

  bool isExplain() { return m_result_set->isExplain(); }

  bool isGeoColOnGpu(const size_t col_idx) {
    return m_result_set->isGeoColOnGpu(col_idx);
  }

  int getDeviceId() { return m_result_set->getDeviceId(); }

  const bool isPermutationBufferEmpty() {
    return m_result_set->isPermutationBufferEmpty();
  }

  size_t getLimit() { return m_result_set->getLimit(); }

  // ColumnType getColType(uint32_t col_num) {
  //   if (col_num < getColCount()) {
  //     SQLTypeInfo type_info = m_result_set->getColType(col_num);
  //     switch (type_info.get_type()) {
  //       case kNUMERIC:
  //       case kDECIMAL:
  //       case kINT:
  //       case kSMALLINT:
  //       case kBIGINT:
  //         return ColumnType::Integer;

  //       case kDOUBLE:
  //         return ColumnType::Double;

  //       case kFLOAT:
  //         return ColumnType::Float;

  //       case kCHAR:
  //       case kVARCHAR:
  //       case kTEXT:
  //         return ColumnType::String;

  //       default:
  //         return ColumnType::Unknown;
  //     }
  //   }
  //   return ColumnType::Unknown;
  // }

 private:
  std::shared_ptr<ResultSet> m_result_set;
  std::weak_ptr<Data_Namespace::DataMgr> m_data_mgr;
};

/**
 * DBEngine internal implementation
 */
class DBEngineImpl : public DBEngine {
 public:
  // TODO: Remove all that hardcoded settings
  const int CALCITEPORT = 3279;
  const std::string OMNISCI_DEFAULT_DB = "omnisci";
  const std::string OMNISCI_ROOT_USER = "admin";
  const std::string OMNISCI_DATA_PATH = "//mapd_data";

  void reset() {
    // TODO: Destroy all cursors in the m_Cursors
    if (m_query_runner != nullptr) {
      m_query_runner->reset();
    }
  }

  void executeDDL(const std::string& query) {
    if (m_query_runner != nullptr) {
      m_query_runner->runDDLStatement(query);
    }
  }

  Cursor* executeDML(const std::string& query) {
    if (m_query_runner != nullptr) {
      auto rs = m_query_runner->runSQL(query, ExecutorDeviceType::CPU);
      m_cursors.emplace_back(new CursorImpl(rs, m_data_mgr));
      return m_cursors.back();
    }
    return nullptr;
  }

  DBEngineImpl(const std::string& base_path)
      : m_base_path(base_path), m_query_runner(nullptr) {
    if (!boost::filesystem::exists(m_base_path)) {
      std::cerr << "Catalog basepath " + m_base_path + " does not exist.\n";
      // TODO: Create database if it does not exist
    } else {
      MapDParameters mapd_parms;
      std::string data_path = m_base_path + OMNISCI_DATA_PATH;
      m_data_mgr =
          std::make_shared<Data_Namespace::DataMgr>(data_path, mapd_parms, false, 0);
      auto calcite = std::make_shared<Calcite>(-1, CALCITEPORT, m_base_path, 1024, 5000);
      auto& sys_cat = Catalog_Namespace::SysCatalog::instance();
      sys_cat.init(m_base_path, m_data_mgr, {}, calcite, false, false, {});
      if (!sys_cat.getSqliteConnector()) {
        std::cerr << "SqliteConnector is null " << std::endl;
      } else {
        sys_cat.getMetadataForDB(OMNISCI_DEFAULT_DB, m_database);  // TODO: Check
        auto catalog = Catalog_Namespace::Catalog::get(m_base_path,
                                                       m_database,
                                                       m_data_mgr,
                                                       std::vector<LeafHostInfo>(),
                                                       calcite,
                                                       false);
        sys_cat.getMetadataForUser(OMNISCI_ROOT_USER, m_user);
        auto session = std::make_unique<Catalog_Namespace::SessionInfo>(
            catalog, m_user, ExecutorDeviceType::CPU, "");
        m_query_runner = QueryRunner::QueryRunner::init(session);
      }
    }
  }

 private:
  std::string m_base_path;
  std::shared_ptr<Data_Namespace::DataMgr> m_data_mgr;
  Catalog_Namespace::DBMetadata m_database;
  Catalog_Namespace::UserMetadata m_user;
  QueryRunner::QueryRunner* m_query_runner;
  std::vector<CursorImpl*> m_cursors;
};

/********************************************* DBEngine external methods*/

/**
 * Creates DBEngine instance
 *
 * @param sPath Path to the existing database
 */
DBEngine* DBEngine::create(std::string path) {
  return new DBEngineImpl(path);
}

/** DBEngine downcasting methods */
inline DBEngineImpl* getImpl(DBEngine* ptr) {
  return (DBEngineImpl*)ptr;
}
inline const DBEngineImpl* getImpl(const DBEngine* ptr) {
  return (const DBEngineImpl*)ptr;
}

void DBEngine::reset() {
  // TODO: Make sure that dbengine does not released twice
  DBEngineImpl* engine = getImpl(this);
  engine->reset();
}

void DBEngine::executeDDL(std::string query) {
  DBEngineImpl* engine = getImpl(this);
  engine->executeDDL(query);
}

Cursor* DBEngine::executeDML(std::string query) {
  DBEngineImpl* engine = getImpl(this);
  return engine->executeDML(query);
}

/********************************************* Row methods */

Row::Row() : m_row() {}

Row::Row(const std::vector<TargetValue>& row) : m_row(row) {}

int64_t Row::getIntScalarTargetValue(size_t col_idx) {
  if (col_idx < m_row.size()) {
    const auto scalar_value = boost::get<ScalarTargetValue>(&m_row[col_idx]);
    const auto value = boost::get<int64_t>(scalar_value);
    return *value;
  }
  std::cout << "Index is out of bound" << std::endl;
  return static_cast<int64_t>(-1);
}

float Row::getFloatScalarTargetValue(size_t col_idx) {
  if (col_idx < m_row.size()) {
    const auto scalar_value = boost::get<ScalarTargetValue>(&m_row[col_idx]);
    const auto value = boost::get<float>(scalar_value);
    return *value;
  }
  std::cout << "Index is out of bound" << std::endl;
  return -1.0F;
}

double Row::getDoubleScalarTargetValue(size_t col_idx) {
  if (col_idx < m_row.size()) {
    const auto scalar_value = boost::get<ScalarTargetValue>(&m_row[col_idx]);
    const auto value = boost::get<double>(scalar_value);
    return *value;
  }
  std::cout << "Index is out of bound" << std::endl;
  return -1.0;
}

std::string Row::getStrScalarTargetValue(size_t col_idx) {
  if (col_idx < m_row.size()) {
    const auto scalar_value = boost::get<ScalarTargetValue>(&m_row[col_idx]);
    auto value = boost::get<NullableString>(scalar_value);
    bool is_null = !value || boost::get<void*>(value);
    if (is_null) {
      return "Nullable string";
    } else {
      auto not_nullable_value = boost::get<std::string>(value);
      return *not_nullable_value;
    }
  }
  std::cout << "Index is out of bound" << std::endl;
  return "";
}

/********************************************* Cursor external methods*/

/** Cursor downcasting methods */
inline CursorImpl* getImpl(Cursor* ptr) {
  return (CursorImpl*)ptr;
}
inline const CursorImpl* getImpl(const Cursor* ptr) {
  return (const CursorImpl*)ptr;
}

ExecutorDeviceType Cursor::getDeviceType() {
  CursorImpl* cursor = getImpl(this);
  return cursor->getDeviceType();
}

Row Cursor::getNextRow(const bool translate_strings, const bool decimal_to_double) {
  CursorImpl* cursor = getImpl(this);
  return cursor->getNextRow(translate_strings, decimal_to_double);
}

OneIntegerColumnRow Cursor::getOneColRow(const size_t index) {
  CursorImpl* cursor = getImpl(this);
  return cursor->getOneColRow(index);
}

size_t Cursor::colCount() {
  CursorImpl* cursor = getImpl(this);
  return cursor->colCount();
}

size_t Cursor::rowCount(const bool force_parallel) {
  CursorImpl* cursor = getImpl(this);
  return cursor->rowCount(force_parallel);
}

void Cursor::setCachedRowCount(const size_t row_count) {
  CursorImpl* cursor = getImpl(this);
  return cursor->setCachedRowCount(row_count);
}

size_t Cursor::entryCount() {
  CursorImpl* cursor = getImpl(this);
  return cursor->entryCount();
}

bool Cursor::definitelyHasNoRows() {
  CursorImpl* cursor = getImpl(this);
  return cursor->definitelyHasNoRows();
}

int8_t* Cursor::getDeviceEstimatorBuffer() {
  CursorImpl* cursor = getImpl(this);
  return cursor->getDeviceEstimatorBuffer();
}

int8_t* Cursor::getHostEstimatorBuffer() {
  CursorImpl* cursor = getImpl(this);
  return cursor->getHostEstimatorBuffer();
}

void Cursor::setQueueTime(const int64_t queue_time) {
  CursorImpl* cursor = getImpl(this);
  return cursor->setQueueTime(queue_time);
}

int64_t Cursor::getQueueTime() {
  CursorImpl* cursor = getImpl(this);
  return cursor->getQueueTime();
}

int64_t Cursor::getRenderTime() {
  CursorImpl* cursor = getImpl(this);
  return cursor->getRenderTime();
}

bool Cursor::isTruncated() {
  CursorImpl* cursor = getImpl(this);
  return cursor->isTruncated();
}

bool Cursor::isExplain() {
  CursorImpl* cursor = getImpl(this);
  return cursor->isExplain();
}

bool Cursor::isGeoColOnGpu(const size_t col_idx) {
  CursorImpl* cursor = getImpl(this);
  return cursor->isGeoColOnGpu(col_idx);
}

int Cursor::getDeviceId() {
  CursorImpl* cursor = getImpl(this);
  return cursor->getDeviceId();
}

const bool Cursor::isPermutationBufferEmpty() {
  CursorImpl* cursor = getImpl(this);
  return cursor->isPermutationBufferEmpty();
}

size_t Cursor::getLimit() {
  CursorImpl* cursor = getImpl(this);
  return cursor->getLimit();
}

// int Cursor::getColType(uint32_t col_num) {
//   CursorImpl* cursor = getImpl(this);
//   return (int)cursor->getColType(col_num);
// }

}  // namespace EmbeddedDatabase
