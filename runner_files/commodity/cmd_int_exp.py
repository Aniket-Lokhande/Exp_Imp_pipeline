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

class CmdIntExp(commodity_runner):
    def __init__(self, run_year: str, conn: Optional[connection]=None):
        super(CmdIntExp, self).__init__(conn)

        self.run_year: str = run_year


    def run(self):

        sql_vars = get_palceholders(self.run_year, 0)

        output_tb = sql_vars['cmd_otpt_tb_exp']
        source_tb = sql_vars['cmd_scr_tb_exp']

        insert_sql = f"""
            SELECT 
                st.hscode, st.commodity, st.year,
                replace(st.exp_inr_cr, ',', '')::NUMERIC AS exp_inr_cr
            FROM {source_tb} st"""

        df = self.execute_sql(insert_sql)

        self.write_table(df, output_tb, 'int', 0)