import os
import re
import sys
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine,Table,Column,Integer,String,MetaData,ForeignKey
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from IPython.display import clear_output

class Insert_CSV_DB():
    
    def __init__(self,table,col,csv="",df=None,dblink,schema):
        self.table = table
        self.col = col #update_col_on_duplicate_key
        self.csv = csv
        self.df = df
        self.dblink = dblink
        self.schema = schema
        self.engine=create_engine(self.dblink + self.schema, echo=False)
        self.DBSession = sessionmaker(bind=self.engine)
       
    def empty_tmp_data(self):
        session = self.DBSession()
        sql = "TRUNCATE TABLE " + self.table + "_tmp"
        session.execute(sql)
        session.commit()
        session.close()
        return

    def insert_data(self):
        self.empty_tmp_data()
        
        if self.csv:
            df = pd.read_csv(self.csv)
        else:
            df = self.df

        df.to_sql(self.table+"_tmp", self.engine, if_exists='append', index=False)
        session = self.DBSession()
        session.commit()
        sql = "INSERT INTO " + self.table + " SELECT * FROM " + self.table + "_tmp ON DUPLICATE KEY UPDATE " \
        + self.table + "." + self.col + "=" + self.table + "_tmp." + self.col
        session.execute(sql)
        session.commit()
        session.close()
    
        self.empty_tmp_data()
