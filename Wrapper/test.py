import dbe
import ctypes
import pandas
ctypes._dlopen('libDBEngine.so', ctypes.RTLD_GLOBAL)
obj = dbe.PyDbEngine('data')
#res = obj.executeDML('SELECT id, abbr, name FROM omnisci_states')
#print('Rows in ResultSet: ', res.rowCount())
#arr = res.getArrowRecordBatch()
#print('Columns in pyarrow RecordBatch: ', arr.columns)
#print('Rows in pyarrow RecordBatch: ', arr.num_rows)
#df = arr.to_pandas()
df = obj.select_df('SELECT id, abbr, name FROM omnisci_states')
print(df)