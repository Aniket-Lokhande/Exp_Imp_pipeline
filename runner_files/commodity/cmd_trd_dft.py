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


class CmdTrdDft(commodity_runner):
    def __init__(self, run_year:str, conn: Optional[connection]=None):
        super(CmdTrdDft, self).__init__(conn)
        self.run_year: str = run_year


    def run(self):

        sql_nm = 'cmd_trd_dft.sql'

        base_year = 2019
        n = int(self.run_year) -  base_year

        if(n<0): return

        if(i==0): 
            pct_change_yr_compr = 0
        else: 
            pct_change_yr_compr = sql_vars['pct_change_yr_compr']

        for i in range(0,n+1):
            if(int(self.run_year) > base_year and i==0): continue
            if(i>10): break
            
            sql_vars = get_palceholders(self.run_year, i)

            sql_vars = {
                'run_yr': sql_vars['run_yr'],
                'prv_yr': sql_vars['prv_yr'],
                'cmd_int_exp_tb': sql_vars['cmd_int_exp_tb'],
                'cmd_int_imp_tb': sql_vars['cmd_int_imp_tb'],
                'cmd_trd_dft_tb': sql_vars['cmd_trd_dft_tb'],
                'pct_change_yr_compr': pct_change_yr_compr
            }

            output_tb = sql_vars['cmd_trd_dft_tb']

            sql_query = self.get_sql(sql_nm, sql_vars)

            df = self.execute_sql(sql_query)

            self.write_table(df, output_tb, 'sum', i)