"""Dominik's ORM"""

import pyodbc

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

    def connect_locally(self):
        self.connection = pyodbc.connect(
            r'DRIVER={ODBC Driver 13 for SQL Server};'
            r'SERVER=' + self.DB_SERVER + ';'
            r'DATABASE=' + self.DB_DATABASE + ';'
            r'TRUSTED_CONNECTION=yes;'
            #r'PWD=' + self.DB_PASSWORD + '') 
        )
        self.cursor = self.connection.cursor()
    
    
    def execute(self,query):
        self.cursor.execute(query)
        self.connection.commit()
        

class Table:
    def __init__(self,db1,name,columns,types):
        self.db1=db1
        self.name=name
        self.columns=columns
        self.types=types
        
        

    def create(self):
        try:
            query="CREATE TABLE "+self.name+"(id INT IDENTITY(1,1) NOT NULL,"
            assert len(self.columns)==len(self.types)
            for i,column in enumerate(self.columns):
                query+=self.columns[i]+" "+self.types[i]+","
            query+="PRIMARY KEY(id))"        
            print(query)
            self.db1.execute(query)
        except Exception as e:
            print("Table "+self.name+" already exists:",e)
        
    def drop(self):
        query="DROP TABLE "+self.name
        self.db1.execute(query)

    def insert(self,rows,batch=50):
        assert len(self.columns)==len(self.types)
        for k in range(len(rows)):
            if k%batch==0:
                query="INSERT INTO "+self.name+" ("
                for i,type in enumerate(self.columns):
                    
                    if i<len(rows[k]):
                        query+=self.columns[i]+","
                if len(rows)<len(self.columns):
                    print(len(self.columns)-len(rows),"columns were not specified")
                query=query[:-1]+") VALUES "
            
            query+="("
            for j in range(len(rows[k])):
                if "nvarchar" in self.types[j]:    
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
        
    
    def select(self,query):
        """given SELECT query returns Python list"""
        """Columns give the number of selected columns"""
        self.db1.cursor.execute(query)
        column_string=query.lower().split("from")[0]
        if "*" in column_string:
            columns=len(self.columns)+1
        else:
            columns = len(column_string.split(","))+1
        print(columns)
        rows = self.db1.cursor.fetchall()
        if columns==1:
            cleared_rows_list = [item[0] for item in rows]
        
        if columns>1:
            cleared_rows_list=[]
            for row in rows: #Because of unhashable type: 'pyodbc.Row'
                list1=[]    
                for i in range(columns):
                    list1.append(row[i])
                cleared_rows_list.append(list1)  
        return(cleared_rows_list)
     
    def select_all(self):
        list1=self.select("SELECT * FROM "+self.name)
        return(list1)
        