from cython.operator cimport dereference as deref
from libcpp.memory cimport unique_ptr
from libcpp.string cimport string
from libc.stdint cimport int64_t, uint64_t, uint32_t
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
from pyarrow.lib cimport *

cdef class PyRow:
    cdef _Row c_row  #Hold a C++ instance which we're wrapping

    def getInt(self, col):
        return self.c_row.getInt(col);

    def getDouble(self, col):
        return self.c_row.getDouble(col);

    def getStr(self, col):
        return self.c_row.getStr(col);


cdef class PyCursor:
    cdef _Cursor* c_cursor  #Hold a C++ instance which we're wrapping
    cdef shared_ptr[CRecordBatch] c_batch

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

    def showRows(self, int max_rows):
        col_count = self.colCount();
        row_count = self.rowCount();
        if row_count > max_rows:
            row_count = max_rows
        col_types = [];
        col_types_str = [];
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
                    fields.append('ARRAY');
                else:
                    """fields.append(r.getStr(f));"""
                    fields.append('UNKNOWN');
#            print(format_row.format("", *fields))
            print(*fields)

    def getArrowRecordBatch(self):
        print('getArrowRecordBatch - BEGIN')
        self.c_batch = self.c_cursor.getArrowRecordBatch()
        print('getArrowRecordBatch - END')
        if self.c_batch.get() is NULL:
            print('Record batch is NULL')
#            return 55555
#        return self.c_batch.get().num_rows()
#        return pyarrow_wrap_batch(self.c_batch.get())
#        dir(pa)
        return pyarrow_wrap_batch(self.c_batch)

cdef class PyDbEngine:
    cdef DBEngine* c_dbe  #Hold a C++ instance which we're wrapping

    def __cinit__(self, path):
        self.c_dbe = DBEngine.create(path)

    def __dealloc__(self):
        self.c_dbe.reset()
        del self.c_dbe

    def executeDDL(self, query):
        try:
            t1=datetime.utcnow()
            self.c_dbe.executeDDL(query)
            t2=datetime.utcnow()
            d = t2-t1
            print(d)
        except Exception, e:
            os.abort()

    def executeDML(self, query):
        obj = PyCursor();
        obj.c_cursor = self.c_dbe.executeDML(query);
        return obj;

    def select_df(self, query):
        obj = PyCursor();
        obj.c_cursor = self.c_dbe.executeDML(query);
        return obj.getArrowRecordBatch().to_pandas()
