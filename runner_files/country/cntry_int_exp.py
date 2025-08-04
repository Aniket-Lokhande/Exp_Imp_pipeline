import pandas as pd
import os
import sys
import psycopg2
import argparse
from typing import Optional
from psycopg2.extensions import connection

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from utils import get_palceholders
from commodity_runner import commodity_runner

class CntryIntExp(commodity_runner):
    def __init__(self, run_year: str, conn: Optional[connection]=None):
        super(CntryIntExp, self).__init__(conn)

        self.run_year: str = run_year


    def run(self):

        sql_vars = get_palceholders(self.run_year, 0)

        source_tb = sql_vars['cntry_scr_tb_exp']
        output_tb = sql_vars['cntry_otpt_tb_exp']
        cntry_cd_mapping = sql_vars['cntry_cd_mapping']

        insert_sql = f"""
            SELECT 
                sr.country, sr.year, cntry_mp.cntry_cd as cntry_cd,
                replace(sr.exp_inr_cr, ',', '')::NUMERIC AS exp_inr_cr
            FROM {source_tb} sr
            join {cntry_cd_mapping} cntry_mp
            on sr.country = cntry_mp.country
            """
        
        df = self.execute_sql(insert_sql)

        self.write_table(df, output_tb, 'int', 0)