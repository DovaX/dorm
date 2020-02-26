import dogui.dogui_core as dg
import dorm as dm

db1=dm.Mysqldb()

def gui_create_table():
    name=entry1.text.get()
    columns=entry2.text.get().split(",")
    types=entry3.text.get().split(",")
    
    table1=dm.MysqlTable(db1,name,columns,types)
    table1.create()
    
def gui_drop_table():
    name=entry1.text.get()
    table1=dm.MysqlTable(db1,name)
    table1.drop()
    
gui1=dg.GUI(title="Dorm Builder GUI")
dg.Label(gui1.window,"Name",1,1)
dg.Label(gui1.window,"Columns",1,2)
dg.Label(gui1.window,"Types",1,3)

entry1=dg.Entry(gui1.window,2,1,width=15)
entry2=dg.Entry(gui1.window,2,2,width=50)
entry3=dg.Entry(gui1.window,2,3,width=50)
dg.Button(gui1.window,"Create Table",gui_create_table,3,1)
dg.Button(gui1.window,"Drop Table",gui_drop_table,3,2)

gui1.build_gui()