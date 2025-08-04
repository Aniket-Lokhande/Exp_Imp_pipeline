from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
import datetime
import psycopg2
from psycopg2.extensions import connection
import os
import sys
import re
import pandas as pd

from utils import PROJECT_ROOT
from pathlib import Path

class base_runner(ABC):
    def __init__(self, conn: Optional[connection]=None):
        self._conn = conn
        self.timestamp = datetime.datetime.now()

    def __enter__(self) -> 'base_runner':
        if self._conn is None:
            params = self.get_config()

            self._conn = psycopg2.connect(
                host=params['host'],
                dbname=params['dbname'],
                user=params['user'],
                password=params['password'],
                port=params['port'],
            )
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        if self._conn:
            self._conn.close()
    
    @property
    def conn(self):
        if self._conn is None:
            print('no active connection to DB')
        
        return self
    
    @abstractmethod
    def get_config(self) -> Dict[str, Any]:
        """ to get connection parameters
            to be overriden by subclasses
        """
        pass


    def read_sql(self, sql_nm):

        file_path = os.path.join(Path(PROJECT_ROOT), 'sqls', sql_nm)

        with open(file_path, 'r', encoding='utf-8') as f:
            sql_query = f.read()

        return sql_query


    def get_sql(self, sql_nm, sql_vars):
        
        sql_query = self.read_sql(sql_nm)
        
        for var in sql_vars:
            pattern = rf"{{\s*{re.escape(var)}\s*}}"

            sql_query = re.sub(
                pattern, "{" + var.upper() + "}", sql_query, flags=re.IGNORECASE
            )

        sql_vars = {key.upper(): value for key, value in sql_vars.items()}

        sql_query = sql_query.format(**sql_vars)

        missing_placeholders = set(re.findall(r"\{(\w+)}", sql_query))

        if(len(missing_placeholders)>0):
            print("missing placeholders")
        
        return sql_query
    
    
    def execute_sql(self, sql_query:str):

        cur = self._conn.cursor()

        cur.execute(sql_query)

        print(sql_query)

        rows = cur.fetchall()

        columns = [desc[0] for desc in cur.description]

        pandas_df = pd.DataFrame(rows, columns=columns)
        cur.close()

        return pandas_df
    

    def write_table(self, df, output_tb, type, num):
        cur = self._conn.cursor()

        prv_yr = str(int(self.run_year)-10)
        prv_yr_1 = str(int(self.run_year)-(10+1))
        prv_10_year = prv_yr_1 + prv_yr[2:]

        run_yr = str(self.run_year)
        run_yr =  str(int(run_yr)-1)+ run_yr[2:]

        if type == 'int':
            dql_sql_cur = f"""DELETE FROM {output_tb} WHERE YEAR = {run_yr}"""
            cur.execute(dql_sql_cur)
            print(dql_sql_cur)

            del_sql_prv = f"""DELETE FROM {output_tb} WHERE YEAR < {prv_10_year}"""
            cur.execute(dql_sql_cur)
            print(del_sql_prv)
        
        elif type=='sum':
            dql_sql_cur = f"""DELETE FROM {output_tb} WHERE YEAR = {run_yr} AND pct_change_yr_compr = {num}"""
            cur.execute(dql_sql_cur)
            print(dql_sql_cur)

            del_sql_prv = f"""DELETE FROM {output_tb} WHERE YEAR < {prv_10_year} AND pct_change_yr_compr = {num}"""
            cur.execute(dql_sql_cur)
            print(del_sql_prv)


        for index, row in df.iterrows():
            placeholders =  ', '.join(['%s'] * len(row))
            columns = ', '.join(row.index)
            sql= f"INSERT INTO {output_tb} ({columns}) VALUES ({placeholders})"
            cur.execute(sql, tuple(row))
        
        self._conn.commit()