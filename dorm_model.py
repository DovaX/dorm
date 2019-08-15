"""Sample model"""

db1=db()

table1 = Table(db1,"test",["test1","test2","test3","test4"],["int","int","int","int"])
table1.create()
rows=[[1,2,3,4],[5,4,7,9]]
table1.insert(rows)
list1=table1.select("SELECT test1 FROM testovaci")
print(list1)

list2=table1.select_all()
print(list2)

table1.drop()
