import dbe
import ctypes
import pandas
ctypes._dlopen('libDBEngine.so', ctypes.RTLD_GLOBAL)
obj = dbe.PyDbEngine('data')
#sch = obj.get_table_details('omnisci_states')
sch = pandas.DataFrame(
    [
        (
            x.name,
            x.type,
            x.precision,
            x.scale,
            x.comp_param,
            x.encoding,
        )
        for x in obj.get_table_details('omnisci_states')
    ],
    columns=[
        'column_name',
        'type',
        'precision',
        'scale',
        'comp_param',
        'encoding',
    ],
)
print(sch)
#res = obj.executeDML('SELECT id, abbr, name FROM omnisci_states')
#print('Rows in ResultSet: ', res.rowCount())
#res.showRows(15)
#arr = res.getArrowRecordBatch()
#print('Columns in pyarrow RecordBatch: ', arr.columns)
#print('Rows in pyarrow RecordBatch: ', arr.num_rows)
#df = arr.to_pandas()
#df = obj.select_df('SELECT id, abbr, name FROM omnisci_states')
#print(df)