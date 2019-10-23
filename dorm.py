"""Dominik's ORM"""

import pyodbc
import pandas as pd

def read_file(file):
    """Reads txt file -> list"""
    with open(file,"r") as f:
        rows = f.readlines()
        for i,row in enumerate(rows):
            rows[i]=row.replace("\n","")
    return(rows)


class db():
    def __init__(self):        
        connection_details=read_file("config.ini")
        db_details={}
        for detail in connection_details:
            key=detail.split("=")[0]
            value=detail.split("=")[1]
            db_details[key]=value
        print(connection_details)
        locally=True
        if db_details["LOCALLY"]=="False":
            locally=False
            
        if locally:
            self.DB_SERVER=db_details["DB_SERVER"]
            self.DB_DATABASE=db_details["DB_DATABASE"]
            self.connect_locally()
        else:
            self.DB_SERVER = db_details["DB_SERVER"]
            self.DB_DATABASE = db_details["DB_DATABASE"]
            self.DB_USERNAME = db_details["DB_USERNAME"]
            self.DB_PASSWORD = db_details["DB_PASSWORD"]
            self.connect_remotely()
             
    def connect_remotely(self):
        self.connection = pyodbc.connect(
            r'DRIVER={ODBC Driver 13 for SQL Server};'
            r'SERVER=' + self.DB_SERVER + ';'
            r'DATABASE=' + self.DB_DATABASE + ';'
            r'UID=' + self.DB_USERNAME + ';'
            r'PWD=' + self.DB_PASSWORD + ''
        )         
        self.cursor = self.connection.cursor()
        print("DB connection established")

    def connect_locally(self):
        self.connection = pyodbc.connect(
            r'DRIVER={ODBC Driver 13 for SQL Server};'
            r'SERVER=' + self.DB_SERVER + ';'
            r'DATABASE=' + self.DB_DATABASE + ';'
            r'TRUSTED_CONNECTION=yes;'
            #r'PWD=' + self.DB_PASSWORD + '') 
        )
        self.cursor = self.connection.cursor()
        print("DB connection established")
    
    def close_connection(self):
        self.connection.close()
        print("DB connection closed")
    
    def execute(self,query):
        self.cursor.execute(query)
        self.cursor.commit()
        #self.connection.commit()
        
        
    def get_all_tables(self):
        sysobjects_table=Table(self, "sysobjects",["name"],["nvarchar(100)"])
        query="select name from sysobjects where xtype='U'"
        rows=sysobjects_table.select(query)
        return(rows)

    def generate_table_dict(self):
        
        tables=self.get_all_tables()
        table_dict=dict()
        for i,table in enumerate(tables):
            table_dict[table]=Table.init_all_columns(self,table)

        return(table_dict)
        
        
class Selectable: #Tables and results of joins
    def __init__(self,db1,name,columns=None):  
        self.db1=db1
        self.name=name
        self.columns=columns
    
    def select(self,query):
        """given SELECT query returns Python list"""
        """Columns give the number of selected columns"""
        self.db1.cursor.execute(query)
        column_string=query.lower().split("from")[0]
        if "*" in column_string:
            columns=len(self.columns)+1
        elif column_string.find(",") == -1:
            columns = 1
        else:
            columns = len(column_string.split(","))
        rows = self.db1.cursor.fetchall()
        if columns==1:
            cleared_rows_list = [item[0] for item in rows]
        
        if columns>1:
            cleared_rows_list=[]
            for row in rows: #Because of unhashable type: 'pyodbc.Row'
                list1=[]
                for i in range(columns):
                    print(row)
                    list1.append(row[i])
                cleared_rows_list.append(list1)  
        return(cleared_rows_list)
     
    def select_all(self):
        list1=self.select("SELECT * FROM "+self.name)
        return(list1)

    
class Joinable(Selectable):
    def __init__(self,db1,name,columns=None):
        super().__init__(db1,name,columns)
    
    def inner_join(self,joinable,column1,column2):
        join_name=self.name+" INNER JOIN "+joinable.name+" ON "+column1+"="+column2
        join_columns=list(set(self.columns) | set(joinable.columns))
        new_joinable=Joinable(self.db1,join_name,join_columns)
        return(new_joinable)
    


class Table(Joinable):
    def __init__(self,db1,name,columns=None,types=None):
        super().__init__(db1,name,columns)
        self.types=types
        
        
    @classmethod
    def init_all_columns(cls,db1,name):
        temporary_table=cls(db1,name)
        columns=temporary_table.get_all_columns()
        types=temporary_table.get_all_types()
        return(cls(db1,name,columns,types))
        
    def get_all_columns(self):
        information_schema_table=Table(self.db1,'INFORMATION_SCHEMA.COLUMNS')
        query="SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME  = '"+self.name+"'"
        columns=information_schema_table.select(query)
        return(columns)

    def get_all_types(self):
        information_schema_table=Table(self.db1,'INFORMATION_SCHEMA.COLUMNS',['DATA_TYPE'],['nvarchar(50)'])
        query="SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME  = '"+self.name+"'"
        types=information_schema_table.select(query)
        return(types)


    def create(self):
        assert len(self.columns)==len(self.types)
        assert self.columns[0]=="id"
        assert self.types[0]=="int"
        query="CREATE TABLE "+self.name+"(id INT IDENTITY(1,1) NOT NULL,"
        for i in range(1,len(self.columns)):
            query+=self.columns[i]+" "+self.types[i]+","
        query+="PRIMARY KEY(id))"        
        print(query)
        try:
            self.db1.execute(query)
        except Exception as e:
            print("Table "+self.name+" already exists:",e)
            print("Check the specification of table columns and their types")
        
    def drop(self):
        query="DROP TABLE "+self.name
        self.db1.execute(query)

    def insert(self,rows,batch=50,replace_apostrophes=True):
        assert len(self.columns)==len(self.types)
        for k in range(len(rows)):
            if k%batch==0:
                query="INSERT INTO "+self.name+" ("
                for i in range(len(1,self.columns)):
                    if i<len(rows[k]):
                        query+=self.columns[i]+","
                if len(rows)<len(self.columns):
                    print(len(self.columns)-len(rows),"columns were not specified")
                query=query[:-1]+") VALUES "
            
            query+="("
            for j in range(len(rows[k])):
                
                if "nvarchar" in self.types[j]:  
                    if replace_apostrophes:
                        rows[k][j]=rows[k][j].replace("'","") 
                    query+="N'"+str(rows[k][j])+"',"
                elif self.types[j]=="int":
                    query+=str(rows[k][j])+","
                else:
                    query+=str(rows[k][j])+","
                
            query=query[:-1]+"),"
            #print(k,batch,k%batch)
            if k%batch==batch-1 or k==len(rows)-1:
                query=query[:-1]
                print(query)
                self.db1.execute(query)
        
                
    def insert_from_df(self,df):
        print(len(df.columns),len(self.columns))
        assert len(df.columns)==len(self.columns)
        rows=df.values.tolist()
        print(rows)
        self.insert(rows)
        
            
    
    def update(self,column,value,where_statement=""):
        if where_statement=="":
            query="UPDATE "+self.name+" SET "+column+"="+value
        else:
            query="UPDATE "+self.name+" SET "+column+"="+value+" WHERE "+where_statement
        #print(query)
        self.db1.cursor.execute(query)
    
    
    def export_to_xlsx(self):
        list1=self.select_all()
        df1=pd.DataFrame(list1,columns=["id"]+self.columns)
        df1.to_excel("items.xlsx")
        
    
    
    

    
