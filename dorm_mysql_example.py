import dorm as dm
import pandas as pd
db1=dm.Mysqldb()

table1=dm.MysqlTable(db1,"test",["id","test2","test3"],["int","int","int"])
table1.create()

rows=[[2,2]]

df=pd.DataFrame(rows,columns=["test2","test3"])

table1.insert_from_df(df)

rows=table1.select_all()
print(rows)

table1.drop()

db1.close_connection()