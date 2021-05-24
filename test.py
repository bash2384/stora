# -*- coding: utf-8 -*-
"""
Created on Sun May  9 11:01:21 2021

@author: bash
"""
import mysql.connector
import datetime # _{x.strftime('%d_%m_%Y_%Hh%Mm')}
import xlrd

x = datetime.datetime.now()

with xlrd.open_workbook(r"D:\try\Janvier.xls") as wb:
    sheet = wb.sheet_by_index(0)
    rows = []
    for row in sheet.get_rows():
        rows.append([cell.value for cell in row])

class My_sql :
    """                                 # Create connexion
    my_db = mysql.connector.connect(
        host= "127.0.0.1",
        user= "bash",
        password= "bash"
        ) 
    mycursor = my_db.cursor()
    mycursor.execute("CREATE DATABASE RTGS")        # Create data base
    """
                                                    # Connexion to data base
    my_db = mysql.connector.connect(
        host= "127.0.0.1",
        user= "bash",
        password= "bash",
        database= "RTGS"
        )
    mycursor = my_db.cursor()
    
    def __init__(self, tab, data, col, key, key_updat):
        self.tab = tab
        self.data = data
        self.key = key
        self.col = col
        self.key_updat = key_updat

    @staticmethod
    def typ_value(data):                           # return typs values on list
        typ_col=[]
        for i in data[1]:
            if type(i) is str:
                typ_col.append("VARCHAR(255)")
            elif type(i) is int:
                typ_col.append("INT(225)")
            elif type(i) is float:
                typ_col.append("FLOAT(24)")
            elif type(i) is bool:
                typ_col.append("BOOL")

        return typ_col
    @staticmethod        
    def valu_typ(data):                             # return str valu and type
        joined = []
        for val,typ in zip( data[0] , My_sql.typ_value(data) ):
            joined.append(val+" "+typ)
            valu_typ = ", ".join(joined)
        return valu_typ
    @staticmethod    
    def len_values(data):                           # return a len value by %s
        lis_len_valu=[]    
        for valu in range(len(data[0])):
            lis_len_valu.append("%s")
            values =", ".join(lis_len_valu)
        return values

    def create_table (self):                        # Create table
        mydb = My_sql.my_db                             # call cre db
        mycursor = mydb.cursor()                        # init cursor
        
        return mycursor.execute(f"CREATE TABLE {self.tab} (id INT AUTO_INCREMENT PRIMARY KEY, {My_sql.valu_typ(self.data)})")
    
    def drop_tab (self):
        mydb = My_sql.my_db                             
        mycursor = mydb.cursor()
        
        sql = f"DROP TABLE IF EXISTS {self.tab}"

        return mycursor.execute(sql)
    
    def fill (self):
        mydb = My_sql.my_db                             
        mycursor = mydb.cursor()
        
        for i in self.data:
            if type(i) is list or tuple:
                sql = f"INSERT INTO {self.tab} ({','.join(self.data[0])}) VALUES ({My_sql.len_values(self.data)} )"
                val = self.data[2:]

                mycursor.executemany(sql, val)
                return mydb.commit()
            
            else:
                """
                sql = f"INSERT INTO {self.tab} ({','.join(self.data)}) VALUES ({My_sql.len_values(self.data)} )"
                val = self.data[1:]

                mycursor.executemany(sql, val)
                return mydb.commit()
                """
                pass
    
    def search (self):
        mydb = My_sql.my_db                             
        mycursor = mydb.cursor()
        
        sql = f"SELECT * FROM {self.tab} WHERE {self.col} LIKE '{self.key}'"

        mycursor.execute(sql)
        myresult = mycursor.fetchall()
        print(myresult)
        return myresult
        
    def delet_key (self):
        mydb = My_sql.my_db                             
        mycursor = mydb.cursor()
        
        sql = f"DELETE FROM {self.tab} WHERE {self.col} = %s"
        adr = (f"{self.key}", )
        
        mycursor.execute(sql, adr)
        return mydb.commit()
    
    def update (self):
        mydb = My_sql.my_db                             
        mycursor = mydb.cursor()
        
        sql = f"UPDATE {self.tab} SET {self.col} = %s WHERE {self.col} = %s"
        val = (f"{self.key}" , f"{self.key_updat}")
        
        mycursor.execute(sql,val)
        mydb.commit()
    



#tab_rtgs = My_sql ( tab, data, col, key ) # as information
tab_rtgs = My_sql("janvier",rows,"libelle", "cpi","centre precomensation interbancaire") # %searsh%


tab_rtgs.create_table()
tab_rtgs.fill()
#tab_rtgs.delet_key()
#tab_rtgs.search()
#tab_rtgs.drop_tab()
#tab_rtgs.update()














#tab_1 = My_sql("date",row)
#tab_1.create_table()

#tab_2 = My_sql(f"date_{x.strftime('%d_%m_%Y_%Hh%Mm')}",None)
#tab_2.create_table()
"""
fill_data = My_sql("customers",row)
fill_data.create_table()
fill_data.fill()
"""



