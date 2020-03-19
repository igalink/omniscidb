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
from DBEngine cimport ResultSet

cdef class PyResultSet:
    cdef shared_ptr[ResultSet] c_result_set  #Hold a C++ instance which we're wrapping
    cdef ResultSet* c_ptr

    def __cinit__(self):
        self.c_result_set.reset()

#    def sizze(PyResultSet self):
#        return deref(self.c_ptr).colCount()

cdef object PyStruct(shared_ptr[ResultSet] PyResultSet_ptr):
    """Python object factory class taking Cpp ResultSet pointer
    as argument
    """
    # Create new PyResultSet object. This does not create
    # new structure but does allocate a null pointer
    cdef PyResultSet py_wrapper = PyResultSet()
    # Set pointer of cdef class to existing struct ptr
    py_wrapper.c_result_set = PyResultSet_ptr
    # Return the wrapped PyResultSet object with PyResultSet_ptr
    return py_wrapper

cdef class PyRow:
    cdef _Row c_row  #Hold a C++ instance which we're wrapping

    def getInt(self, col):
        return self.c_row.GetInt(col);

    def getDouble(self, col):
        return self.c_row.GetDouble(col);

    def getStr(self, col):
        return self.c_row.GetStr(col);


cdef class PyCursor:
    def colCount(self):
        return self.c_cursor.GetColCount()

    def rowCount(self):
        return self.c_cursor.GetRowCount()

    def nextRow(self):
        obj = PyRow()
        obj.c_row = self.c_cursor.GetNextRow()
        return obj

    def getColType(self, uint32_t pos):
#        obj = PyColumnType()
#        obj.c_col_type = self.c_cursor.GetColType(pos)
        return self.c_cursor.GetColType(pos)

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

cdef class PyDbEngine:
    cdef DBEngine* c_dbe  #Hold a C++ instance which we're wrapping

    def __cinit__(self, path):
        self.c_dbe = DBEngine.Create(path)

    def __dealloc__(self):
        self.c_dbe.Reset()
        del self.c_dbe

    def executeDDL(self, query):
        try:
            t1=datetime.utcnow()
            self.c_dbe.ExecuteDDL(query)
            t2=datetime.utcnow()
            d = t2-t1
            print(d)
        except Exception, e:
            os.abort()

    def executeDML(self, query):
        """        try:"""
        obj = PyCursor();
        obj.c_cursor = self.c_dbe.ExecuteDML(query);
        return obj;
#        obj = PyResultSet()
#        obj.c_ptr = self.c_dbe.ExecuteDML(query).get();
#        return obj;
#        cdef PyResultSet mypystruct = PyStruct(self.c_dbe.ExecuteDML(query))
#        return mypystruct
        """        except Exception, e:
        os.abort()
        """