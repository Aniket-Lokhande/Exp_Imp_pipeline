import os
import sys
from typing import Optional
import psycopg2
from psycopg2.extensions import connection
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from utils import get_palceholders
from country_runner import country_runner

class CntryUniqueCd(country_runner):
    def __init__(self, run_year:str, conn: Optional[connection]=None):
        super(CntryUniqueCd, self).__init__(conn)
        self.run_year = run_year
    
    def run(self):

        sql_vars = get_palceholders(self.run_year, 0)

        cntry_scr_tb_imp = sql_vars['cntry_scr_tb_imp']
        cntry_scr_tb_exp = sql_vars['cntry_scr_tb_exp']
        cntry_cd_mapping = sql_vars['cntry_cd_mapping']

        get_cntry_nm_sql = f"""
            select country
            from (
                select country from {cntry_scr_tb_imp}
                union
                select country from {cntry_scr_tb_exp}
            )
        """

        df_exp_imp_nm = self.execute_sql(get_cntry_nm_sql)

        get_cntry_mapping_sql = f"""
            select distinct cntry_cd, country from {cntry_cd_mapping}"""
        
        df_mapping = self.execute_sql(get_cntry_mapping_sql)

        cntry_cd_st = 1 if df_mapping.empty else df_mapping['cntry_cd'].max()+1

        df_final = df_exp_imp_nm[~df_exp_imp_nm['country'].isin(df_mapping['country'])]

        df_final['cntry_cd'] = range(cntry_cd_st, cntry_cd_st+ len(df_final))
  
        self.write_table(df_final, cntry_cd_mapping, 'cntry', 0)