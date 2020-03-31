from __future__ import absolute_import

from libc.stdint cimport int64_t, uint64_t, uint32_t, int8_t
from libcpp.memory cimport shared_ptr
from libcpp.string cimport string
from libcpp cimport bool, nullptr_t, nullptr
from libcpp.pair cimport pair
from libcpp.vector cimport vector
from libcpp.list cimport list
from cython.operator cimport dereference as deref


cdef extern from "boost/variant.hpp" namespace "boost":
    cdef cppclass boost_variant "boost::variant" [T1]:
        boost_variant()

    cdef cppclass boost_variant2 "boost::variant" [T1, T2]:
        boost_variant2()

    cdef cppclass boost_variant4 "boost::variant" [T1, T2, T3, T4]:
        boost_variant4()

cdef extern from "boost/optional.hpp" namespace "boost":
    cdef cppclass boost_optional "boost::optional" [T]:
        pass

cdef extern from "boost/variant/get.hpp" namespace "boost":
    cdef U * boost_get4 "boost::get" [U, T1, T2, T3, T4](boost_variant4[T1, T2, T3, T4] * operand);
    cdef U * boost_get2 "boost::get" [U, T1, T2](boost_variant2[T1, T2] * operand);

# cdef extern from "../Analyzer/Analyzer.h" namespace "Analyzer":
#     cdef struct AnalyzerOrderEntry "Analyzer::OrderEntry":
#         int tle_no;
#         bool is_desc;
#         bool nulls_first;

cdef extern from "../QueryEngine/TargetValue.h":
    cdef struct GeoPointTargetValue:
        pass

    cdef struct GeoLineStringTargetValue:
        pass

    cdef struct GeoPolyTargetValue:
        pass

    cdef struct GeoMultiPolyTargetValue:
        pass

    cdef struct GeoPointTargetValuePtr:
        pass

    cdef struct GeoLineStringTargetValuePtr:
        pass

    cdef struct GeoPolyTargetValuePtr:
        pass

    cdef struct GeoMultiPolyTargetValuePtr:
        pass

ctypedef boost_variant2[string, void*] NullableString

ctypedef void* VoidPtr

ctypedef boost_variant4[int64_t, double, float, NullableString] ScalarTargetValue

ctypedef boost_optional[vector[ScalarTargetValue]] ArrayTargetValue

ctypedef boost_optional[
    boost_variant4[
        GeoPointTargetValue, GeoLineStringTargetValue, GeoPolyTargetValue, GeoMultiPolyTargetValue
    ]
] GeoTargetValue

ctypedef boost_variant4[
    GeoPointTargetValuePtr, GeoLineStringTargetValuePtr, GeoPolyTargetValuePtr, GeoMultiPolyTargetValuePtr
] GeoTargetValuePtr

ctypedef boost_variant4[ScalarTargetValue, ArrayTargetValue, GeoTargetValue, GeoTargetValuePtr] TargetValue

ctypedef struct CyOneIntegerColumnRow:
        int64_t value
        bool valid

cdef extern from "../QueryEngine/CompilationOptions.h":
    cdef cppclass ExecutorDeviceType:
        pass
    cdef ExecutorDeviceType CPU "ExecutorDeviceType::CPU"
    cdef ExecutorDeviceType GPU "ExecutorDeviceType::GPU"

cdef extern from "../QueryEngine/ResultSet.h":
    cdef struct OneIntegerColumnRow:
        const int64_t value
        const bool valid

    # cdef cppclass ResultSetStorage:
    #     pass

    # cdef cppclass ResultSetRowIterator:
    #     pass

cdef extern from "DBEngine.h" namespace "EmbeddedDatabase":
    cdef cppclass Row:
        int64_t getIntScalarTargetValue(size_t col_idx)
        float getFloatScalarTargetValue(size_t col_idx)
        double getDoubleScalarTargetValue(size_t col_idx)
        string getStrScalarTargetValue(size_t col_idx)

    cdef cppclass Cursor:
        # int getColType(uint32_t nPos)
        # shared_ptr[CRecordBatch] GetArrowRecordBatch()

        ##########################################
        # inline ResultSetRowIterator rowIterator(
        #     size_t from_logical_index,
        #     bool translate_strings,
        #     bool decimal_to_double) const

        # inline ResultSetRowIterator rowIterator(
        #     bool translate_strings,
        #     bool decimal_to_double) const

        ExecutorDeviceType getDeviceType() const

        # const ResultSetStorage * allocateStorage() const

        # const ResultSetStorage * allocateStorage(int8_t*, const vector[int64_t] & ) const

        # const ResultSetStorage * allocateStorage(const vector[int64_t] & ) const

        # void updateStorageEntryCount(const size_t new_entry_count)

        Row getNextRow(
            const bool translate_strings, const bool decimal_to_double
        ) const

        # vector[TargetValue] getRowAt(const size_t index) const

        # TargetValue getRowAt(
            # const size_t row_idx, const size_t col_idx,
            # const bool translate_strings, const bool decimal_to_double
        # ) const

        # *
        OneIntegerColumnRow getOneColRow(const size_t index) const

        # vector[TargetValue] getRowAtNoTranslations(
        #     const size_t index, const vector[bool] & targets_to_skip={}) const

        # bool isRowAtEmpty(size_t index) const

        size_t colCount() const

        size_t rowCount(const bool force_parallel) const

        # size_t getCurrentRowBufferIndex() const

        # void sort(const list[AnalyzerOrderEntry] & order_entries, const size_t top_n)

        # void keepFirstN(const size_t n)

        # void dropFirstN(const size_t n)

        # void append(ResultSet & that)

        # const ResultSetStorage * getStorage() const

        # delete constness
        void setCachedRowCount(const size_t row_count) const

        size_t entryCount() const

        bool definitelyHasNoRows() const

        int8_t * getDeviceEstimatorBuffer() const

        int8_t * getHostEstimatorBuffer() const

        # void syncEstimatorBuffer() const

        # size_t getNDVEstimator() const

        void setQueueTime(const int64_t queue_time)

        int64_t getQueueTime() const

        int64_t getRenderTime() const

        # void moveToBegin() const

        bool isTruncated() const

        bool isExplain() const

        bool isGeoColOnGpu(const size_t col_idx) const

        int getDeviceId() const

        # void initializeStorage() const

        const bool isPermutationBufferEmpty() const

        size_t getLimit()

    cdef cppclass DBEngine:
        void reset()
        void executeDDL(string)
        Cursor * executeDML(string)
        @staticmethod
        DBEngine * create(string)