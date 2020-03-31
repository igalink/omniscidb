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


# cdef class PyResultSetRowIterator:
#     cdef ResultSetRowIterator* c_ptr

#     def __cinit__(self):
#         self.c_ptr = NULL

#     def __dealloc__(self):
#         if self.c_ptr is not NULL:
#             c_ptr = NULL

# cdef class PyVectorInt64:
#     cdef vector[int64_t] c_obj
#
#     def __cinit__(self):
#         c_obj = vector[int64_t]()

# cdef class PyListAnalyzerOrderEntry:
#     cdef list[AnalyzerOrderEntry] c_obj
#
#     def __cinit__(self):
#         c_obj = list[AnalyzerOrderEntry]()

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

# cdef class PyVectorBool:
#     cdef vector[bool] c_obj
#
#     def __cinit__(self):
#         self.c_obj = vector[bool]()


# cdef class PyTargetValue:
#     cdef TargetValue c_obj
#
#     def __cinit__(self):
#         self.c_obj = TargetValue()

cdef class PyExecutorDeviceType:
    cdef ExecutorDeviceType c_dev_type

    def __cinit__(self):
        self.c_dev_type = CPU

    def __getattr__(self, name):
        if name == 'type':
            return < int > self.c_dev_type
        else:
            raise AttributeError

cdef class PyPtrInt8:
    cdef int8_t * c_ptr

    def __cinit__(self):
        self.c_ptr = NULL

    def getElement(self, idx: c_int) -> int8_t:
        if self.c_ptr is NULL:
            return <int8_t>0
        cdef int8_t val = self.c_ptr[idx]
        return val

    def __dealloc__(self):
        if self.c_ptr is not NULL:
            self.c_ptr = NULL

# cdef class PyResultSetStorage:
#     cdef const ResultSetStorage * c_ptr
#
#     def __cinit__(self):
#         self.c_ptr = NULL
#
#     def __dealloc__(self):
#         if self.c_ptr is not NULL:
#             self.c_ptr = NULL


cdef class PyRow:
    cdef _Row c_row  # Hold a C++ instance which we're wrapping

    def getIntScalarTargetValue(self, col_idx: csize_t) -> int64_t:
        return self.c_row.getIntScalarTargetValue(col_idx)

    def getFloatScalarTargetValue(self, col_idx: csize_t) -> c_float:
        return self.c_row.getFloatScalarTargetValue(col_idx)

    def getDoubleScalarTargetValue(self, col_idx: csize_t) -> c_double:
        return self.c_row.getDoubleScalarTargetValue(col_idx)

    def getStrScalarTargetValue(self, col_idx: csize_t) -> c_wchar_p:
        return self.c_row.getStrScalarTargetValue(col_idx)


cdef class PyCursor:
    cdef _Cursor * c_cursor  # Hold a C++ instance which we're wrapping

    def getDeviceType(self) -> PyExecutorDeviceType:
        obj = PyExecutorDeviceType()
        obj.c_dev_type = self.c_cursor.getDeviceType()
        return obj

    def getNextRow(self,
                   translate_strings: c_bool,
                   decimal_to_double: c_bool) -> PyRow:
        obj = PyRow()
        obj.c_row = self.c_cursor.getNextRow(
            translate_strings, decimal_to_double
        )
        return obj

    def getOneColRow(self, index: csize_t) -> PyOneIntegerColumnRow:
        obj = PyOneIntegerColumnRow()
        cdef int64_t value = self.c_cursor.getOneColRow(index).value
        cdef bool valid = self.c_cursor.getOneColRow(index).valid
        obj.set_attr(value, valid)
        return obj

    def colCount(self) -> csize_t:
        return self.c_cursor.colCount()

    def rowCount(self, force_parallel: c_bool = False) -> csize_t:
        return self.c_cursor.rowCount(force_parallel)

    def setCachedRowCount(self, row_count: csize_t):
        self.c_cursor.setCachedRowCount(row_count)

    def entryCount(self) -> csize_t:
        return self.c_cursor.entryCount()

    def definitelyHasNoRows(self) -> c_bool:
        return self.c_cursor.definitelyHasNoRows()

    def getDeviceEstimatorBuffer(self) -> PyPtrInt8:
        obj = PyPtrInt8()
        obj.c_ptr = self.c_cursor.getDeviceEstimatorBuffer()
        return obj

    def getHostEstimatorBuffer(self) -> PyPtrInt8:
        obj = PyPtrInt8()
        obj.c_ptr = self.c_cursor.getHostEstimatorBuffer()
        return obj

    def setQueueTime(self, queue_time: int64_t):
        self.c_cursor.setQueueTime(queue_time)

    def getQueueTime(self) -> int64_t:
        return self.c_cursor.getQueueTime()

    def getRenderTime(self) -> int64_t:
        return self.c_cursor.getRenderTime()

    def isTruncated(self) -> c_bool:
        return self.c_cursor.isTruncated()

    def isExplain(self) -> c_bool:
        return self.c_cursor.isExplain()

    def isGeoColOnGpu(self, col_idx: csize_t) -> c_bool:
        return self.c_cursor.isGeoColOnGpu(col_idx)

    def getDeviceId(self) -> c_int:
        return self.c_cursor.getDeviceId()

    def isPermutationBufferEmpty(self) -> c_bool:
        return self.c_cursor.isPermutationBufferEmpty()

    def getLimit(self) -> csize_t:
        return self.c_cursor.getLimit()

    ##################################
    # def getColType(self, uint32_t pos):
        #        obj = PyColumnType()
        #        obj.c_col_type = self.c_cursor.GetColType(pos)
    #     return self.c_cursor.getColType(pos)

#     def showRows(self, int max_rows):
#         col_count = self.colCount()
#         row_count = self.rowCount()
#         if row_count > max_rows:
#             row_count = max_rows
#         col_types = []
#         col_types_str = []
#         for i in range(col_count):
#             ct = self.getColType(i)
#             col_types.append(ct)
#             if ct == 1:
#                 col_types_str.append('int')
#             elif ct == 2:
#                 col_types_str.append('double')
#             elif ct == 3:
#                 col_types_str.append('float')
#             elif ct == 4:
#                 col_types_str.append('string')
#             elif ct == 5:
#                 col_types_str.append('array')
#             else:
#                 col_types_str.append('Unknown')

#         format_row = "{:>12}" * (len(col_types) + 1)
#         """print(format_row.format("", *col_types))"""
#         print(*col_types_str)
#         for j in range(row_count):
#             r = self.nextRow()
#             fields = []
#             for f in range(col_count):
#                 if col_types[f] == 1:
#                     """ColumnType.eINT:"""
#                     fields.append(r.getInt(f))
#                 elif col_types[f] == 2:
#                     """ColumnType.eDBL:"""
#                     fields.append(r.getDouble(f))
#                 elif col_types[f] == 3:
#                     """ColumnType.eFLT:"""
#                     fields.append('FLOAT')
# #                    fields.append(r.getFloat(f))
#                 elif col_types[f] == 4:
#                     """ColumnType.eSTR:"""
#                     fields.append(r.getStr(f))
#                 elif col_types[f] == 5:
#                     """ColumnType.eARR:"""
#                     fields.append('ARRAY')
#                 else:
#                     """fields.append(r.getStr(f));"""
#                     fields.append('UNKNOWN')
# #            print(format_row.format("", *fields))
#             print(*fields)

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
