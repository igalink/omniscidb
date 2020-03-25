from ctypes import *
from cython.operator cimport dereference as deref
from cython.operator cimport address
from libcpp.memory cimport unique_ptr
from libcpp.string cimport string
from libc.stdint cimport int64_t, uint64_t, uint32_t, int8_t
from libcpp.memory cimport shared_ptr
from libcpp cimport bool
from libcpp.pair cimport pair
from libcpp.vector cimport vector
from datetime import datetime

import os

from DBEngine cimport *
from DBEngine cimport Row as _Row
from DBEngine cimport Cursor as _Cursor
from DBEngine cimport DBEngine
from DBEngine cimport ResultSet as _ResultSet


# cdef class PyResultSetRowIterator:
#     cdef ResultSetRowIterator* c_ptr

#     def __cinit__(self):
#         self.c_ptr = NULL

#     def __dealloc__(self):
#         if self.c_ptr is not NULL:
#             c_ptr = NULL

cdef class PyVectorInt64:
    cdef vector[int64_t] c_obj

    def __cinit__(self):
        c_obj = vector[int64_t]()

cdef class PyListAnalyzerOrderEntry:
    cdef list[AnalyzerOrderEntry] c_obj

    def __cinit__(self):
        c_obj = list[AnalyzerOrderEntry]()

cdef class PyOneIntegerColumnRow:
    cdef CyOneIntegerColumnRow c_obj

    def set_attr(self, value, valid):
        self.c_obj.value = value
        self.c_obj.valid = valid

    def __getattr__(self, name):
        if name == 'value':
            return self.c_obj.value
        elif name == 'valid':
            return self.c_obj.valid
        else:
            raise AttributeError

cdef class PyVectorBool:
    cdef vector[bool] c_obj

    def __cinit__(self):
        self.c_obj = vector[bool]()

cdef class PyVectorTargetValue:
    cdef vector[TargetValue] c_obj

    def __cinit__(self):
        self.c_obj = vector[TargetValue]()

    def getIntScalarTargetValue(self, col_idx):
        if col_idx >= self.c_obj.size():
            raise IndexError

        cdef ScalarTargetValue * scalar_value = boost_get4[
            ScalarTargetValue, ScalarTargetValue,
            ArrayTargetValue, GeoTargetValue, GeoTargetValuePtr](
                address(self.c_obj[col_idx])
            )
        cdef int64_t * value = boost_get4[
            int64_t, int64_t, double, float, NullableString](
                scalar_value
            )
        return deref(value)

    def getFloatScalarTargetValue(self, col_idx):
        if col_idx >= self.c_obj.size():
            raise IndexError

        cdef ScalarTargetValue * scalar_value = boost_get4[
            ScalarTargetValue, ScalarTargetValue,
            ArrayTargetValue, GeoTargetValue, GeoTargetValuePtr](
                address(self.c_obj[col_idx])
            )
        cdef float * value = boost_get4[
            float, int64_t, double, float, NullableString](
                scalar_value
            )
        return deref(value)

    def getDoubleScalarTargetValue(self, col_idx):
        if col_idx >= self.c_obj.size():
            raise IndexError

        cdef ScalarTargetValue * scalar_value = boost_get4[
            ScalarTargetValue, ScalarTargetValue,
            ArrayTargetValue, GeoTargetValue, GeoTargetValuePtr](
                address(self.c_obj[col_idx])
            )
        cdef double * value = boost_get4[
            double, int64_t, double, float, NullableString](
                scalar_value
            )
        return deref(value)

    def getStrScalarTargetValue(self, col_idx):
        if col_idx >= self.c_obj.size():
            raise IndexError

        cdef ScalarTargetValue * scalar_value = boost_get4[
            ScalarTargetValue, ScalarTargetValue,
            ArrayTargetValue, GeoTargetValue, GeoTargetValuePtr](
                address(self.c_obj[col_idx])
            )
        cdef NullableString * value = boost_get4[
            NullableString, int64_t, double, float, NullableString](
                scalar_value
            )
        if not value or not boost_get2[VoidPtr, string, VoidPtr](value):
            is_null = True
        else:
            is_null = False
        if is_null:
            return "Nullable string";
        cdef string * not_nullable_value = boost_get2[string, string, VoidPtr](value)
        return deref(not_nullable_value);

cdef class PyTargetValue:
    cdef TargetValue c_obj

    def __cinit__(self):
        self.c_obj = TargetValue()

cdef class PyExecutorDeviceType:
    cdef ExecutorDeviceType c_obj

    def __cinit__(self):
        self.c_obj = CPU

    def __getattr__(self, name):
        if name == 'value':
            return <int>self.c_obj
        else:
            raise AttributeError

cdef class PyPtrInt8:
    cdef int8_t * c_ptr

    def __cinit__(self):
        self.c_ptr = NULL

    def __get_item__(self, key) -> int8_t:
        return self.c_ptr[key]

    def __dealloc__(self):
        if self.c_ptr is not NULL:
            self.c_ptr = NULL

cdef class PyResultSetStorage:
    cdef const ResultSetStorage * c_ptr

    def __cinit__(self):
        self.c_ptr = NULL

    def __dealloc__(self):
        if self.c_ptr is not NULL:
            self.c_ptr = NULL

cdef class PyResultSet:
    cdef _ResultSet * c_ptr  # Hold a C++ pointer to instance which we're wrapping

    def __cinit__(self):
        self.c_ptr = NULL

    def __dealloc__(self):
        if self.c_ptr is not NULL:
            del self.c_ptr

    # def rowIterator3(self,
    #                  from_logical_index: csize_t,
    #                  translate_strings: c_bool,
    #                  decimal_to_double: c_bool) -> PyResultSetRowIterator:
    #     obj = PyResultSetRowIterator()
    #     obj.c_ptr = address(deref(self.c_ptr).rowIterator(
    #         from_logical_index, translate_strings, decimal_to_double
    #     ))
    #     return obj

    # def rowIterator2(self,
    #                  translate_strings: c_bool,
    #                  decimal_to_double: c_bool) -> PyResultSetRowIterator:
    #     obj = PyResultSetRowIterator()
    #     obj.c_ptr = address(deref(self.c_ptr).rowIterator(
    #         translate_strings, decimal_to_double
    #     ))
    #     return obj

    def getDeviceType(self) -> PyExecutorDeviceType:
        obj = PyExecutorDeviceType()
        obj.c_obj = deref(self.c_ptr).getDeviceType()
        return obj

    def allocateStorage(self) -> PyResultSetStorage:
        obj = PyResultSetStorage()
        obj.c_ptr = deref(self.c_ptr).allocateStorage()
        return obj

    def allocateStorage2(self,
                         param1: PyPtrInt8, param2: PyVectorInt64
                         ) -> PyResultSetStorage:
        obj = PyResultSetStorage()
        obj.c_ptr = deref(self.c_ptr).allocateStorage(param1, param2)
        return obj

    def allocateStorage1(self, param: PyVectorInt64) -> PyResultSetStorage:
        obj = PyResultSetStorage()
        obj.c_ptr = deref(self.c_ptr).allocateStorage(param)
        return obj

    def updateStorageEntryCount(self, new_entry_count: csize_t):
        deref(self.c_ptr).updateStorageEntryCount(new_entry_count)

    def getNextRow(self,
                   translate_strings: c_bool,
                   decimal_to_double: c_bool) -> PyVectorTargetValue:
        obj = PyVectorTargetValue()
        obj.c_obj = deref(self.c_ptr).getNextRow(
            translate_strings, decimal_to_double
        )
        return obj

    def getRowAtV(self, index: csize_t) -> PyVectorTargetValue:
        obj = PyVectorTargetValue()
        obj.c_obj = deref(self.c_ptr).getRowAt(index)
        return obj

    def getRowAt(self,
                 row_idx: csize_t,
                 col_idx: csize_t,
                 translate_strings: c_bool,
                 decimal_to_double: c_bool) -> PyTargetValue:
        obj = PyTargetValue()
        obj.c_obj = deref(self.c_ptr).getRowAt(
            row_idx, col_idx, translate_strings, decimal_to_double
        )
        return obj

    def getOneColRow(self, index: csize_t) -> PyOneIntegerColumnRow:
        obj = PyOneIntegerColumnRow()
        cdef int64_t value = deref(self.c_ptr).getOneColRow(index).value
        cdef bool valid = deref(self.c_ptr).getOneColRow(index).valid
        obj.set_attr(value, valid)
        return obj

    # def getRowAtNoTranslations(self, index: csize_t,
    #                            targets_to_skip: PyVectorBool = PyVectorBool()
    #                            ) -> PyVectorTargetValue:
    #     obj = PyVectorTargetValue()
    #     obj.c_obj = deref(self.c_ptr).getRowAtNoTranslations(
    #         index, targets_to_skip.c_obj
    #     )
    #     return obj

    def isRowAtEmpty(self, index: csize_t) -> c_bool:
        return deref(self.c_ptr).isRowAtEmpty(index)

    def colCount(self) -> csize_t:
        return deref(self.c_ptr).colCount()

    def rowCount(self, force_parallel: c_bool = False) -> csize_t:
        return deref(self.c_ptr).rowCount(force_parallel)

    def getCurrentRowBufferIndex(self) -> csize_t:
        return deref(self.c_ptr).getCurrentRowBufferIndex()

    def sort(self,
             order_entries: PyListAnalyzerOrderEntry,
             top_n: csize_t):
        deref(self.c_ptr).sort(order_entries.c_obj, top_n)

    def keepFirstN(self, n: csize_t):
        deref(self.c_ptr).keepFirstN(n)

    def dropFirstN(self, n: csize_t):
        deref(self.c_ptr).dropFirstN(n)

    def append(self, that: PyResultSet):
        deref(self.c_ptr).append(deref(that.c_ptr))

    def getStorage(self) -> PyResultSetStorage:
        obj = PyResultSetStorage()
        obj.c_ptr = deref(self.c_ptr).getStorage()
        return obj

    def setCachedRowCount(self, row_count: csize_t):
        deref(self.c_ptr).setCachedRowCount(row_count)

    def entryCount(self) -> csize_t:
        return deref(self.c_ptr).entryCount()

    def definitelyHasNoRows(self) -> c_bool:
        return deref(self.c_ptr).definitelyHasNoRows()

    def getDeviceEstimatorBuffer(self) -> PyPtrInt8:
        obj = PyPtrInt8()
        obj.c_ptr = deref(self.c_ptr).getDeviceEstimatorBuffer()
        return obj

    def getHostEstimatorBuffer(self) -> PyPtrInt8:
        obj = PyPtrInt8()
        obj.c_ptr = deref(self.c_ptr).getHostEstimatorBuffer()
        return obj

    def syncEstimatorBuffer(self):
        deref(self.c_ptr).syncEstimatorBuffer()

    def getNDVEstimator(self) -> csize_t:
        return deref(self.c_ptr).getNDVEstimator()

    def setQueueTime(self, queue_time: int64_t):
        deref(self.c_ptr).setQueueTime(queue_time)

    def getQueueTime(self) -> int64_t:
        return deref(self.c_ptr).getQueueTime()

    def getRenderTime(self) -> int64_t:
        return deref(self.c_ptr).getRenderTime()

    def moveToBegin(self):
        deref(self.c_ptr).moveToBegin()

    def isTruncated(self) -> c_bool:
        return deref(self.c_ptr).isTruncated()

    def isExplain(self) -> c_bool:
        return deref(self.c_ptr).isExplain()

    def isGeoColOnGpu(self, col_idx: csize_t) -> c_bool:
        return deref(self.c_ptr).isGeoColOnGpu(col_idx)

    def getDeviceId(self) -> c_int:
        return deref(self.c_ptr).getDeviceId()

    def initializeStorage(self):
        deref(self.c_ptr).initializeStorage()

    def isPermutationBufferEmpty(self) -> c_bool:
        return deref(self.c_ptr).isPermutationBufferEmpty()

    def getLimit(self) -> csize_t:
        return deref(self.c_ptr).getLimit()

cdef class PyRow:
    cdef _Row c_row  # Hold a C++ instance which we're wrapping

    def getInt(self, col):
        return self.c_row.getInt(col)

    def getDouble(self, col):
        return self.c_row.getDouble(col)

    def getStr(self, col):
        return self.c_row.getStr(col)


cdef class PyCursor:
    cdef _Cursor * c_cursor  # Hold a C++ instance which we're wrapping

    def colCount(self):
        return self.c_cursor.getColCount()

    def rowCount(self):
        return self.c_cursor.getRowCount()

    def nextRow(self):
        obj = PyRow()
        obj.c_row = self.c_cursor.getNextRow()
        return obj

    def getColType(self, uint32_t pos):
        #        obj = PyColumnType()
        #        obj.c_col_type = self.c_cursor.GetColType(pos)
        return self.c_cursor.getColType(pos)

    def getResultSet(self):
        obj = PyResultSet()
        obj.c_ptr = self.c_cursor.getResultSet()
        return obj

    def showRows(self, int max_rows):
        col_count = self.colCount()
        row_count = self.rowCount()
        if row_count > max_rows:
            row_count = max_rows
        col_types = []
        col_types_str = []
        for i in range(col_count):
            ct = self.getColType(i)
            col_types.append(ct)
            if ct == 1:
                col_types_str.append('int')
            elif ct == 2:
                col_types_str.append('double')
            elif ct == 3:
                col_types_str.append('float')
            elif ct == 4:
                col_types_str.append('string')
            elif ct == 5:
                col_types_str.append('array')
            else:
                col_types_str.append('Unknown')

        format_row = "{:>12}" * (len(col_types) + 1)
        """print(format_row.format("", *col_types))"""
        print(*col_types_str)
        for j in range(row_count):
            r = self.nextRow()
            fields = []
            for f in range(col_count):
                if col_types[f] == 1:
                    """ColumnType.eINT:"""
                    fields.append(r.getInt(f))
                elif col_types[f] == 2:
                    """ColumnType.eDBL:"""
                    fields.append(r.getDouble(f))
                elif col_types[f] == 3:
                    """ColumnType.eFLT:"""
                    fields.append('FLOAT')
#                    fields.append(r.getFloat(f))
                elif col_types[f] == 4:
                    """ColumnType.eSTR:"""
                    fields.append(r.getStr(f))
                elif col_types[f] == 5:
                    """ColumnType.eARR:"""
                    fields.append('ARRAY')
                else:
                    """fields.append(r.getStr(f));"""
                    fields.append('UNKNOWN')
#            print(format_row.format("", *fields))
            print(*fields)

cdef class PyDbEngine:
    cdef DBEngine * c_dbe  # Hold a C++ instance which we're wrapping

    def __cinit__(self, path: str):
        self.c_dbe = DBEngine.create(path)

    def __dealloc__(self):
        self.c_dbe.reset()
        del self.c_dbe

    def executeDDL(self, query: str):
        try:
            t1 = datetime.utcnow()
            self.c_dbe.executeDDL(query)
            t2 = datetime.utcnow()
            d = t2-t1
            print(d)
        except Exception, e:
            os.abort()

    def executeDML(self, query: str) -> PyCursor:
        try:
            # obj = PyResultSet()
            # obj.c_ptr = deref(self.c_dbe).executeDML(query).get()
            # return obj
            obj = PyCursor()
            obj.c_cursor = self.c_dbe.executeDML(query)
            return obj
            # cdef PyResultSet mypystruct = PyStruct(self.c_dbe.ExecuteDML(query))
            # return mypystruct
        except Exception, e:
            os.abort()
