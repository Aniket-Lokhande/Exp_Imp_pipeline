import os
import re 
import pandas as pd
import psycopg2
import yaml
from pathlib import Path

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))


def read_config() -> dict:
    config_path = Path(PROJECT_ROOT)/'config.yml'

    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def load_conn_params() -> dict:
    params = read_config()["DB_connection"]
    return params


def get_year_format(year, num):

    prv_yr = str(int(year)-num)
    prv_yr_1 = str(int(year)-(num+1))
    prv_yr = prv_yr_1 + prv_yr[2:]

    return prv_yr


def get_palceholders(run_yr, num):
    run_yr = str(run_yr)
    prv_yr_1 = str(int(run_yr)-1)

    tb_config = read_config()["Table_config"]

    # get source and output table for import commodity
    cmd_scr_tb_imp_tb = tb_config['cmd_scr_tb_imp_tb'] + str(prv_yr_1[2:]) + str(run_yr[2:])
    cmd_scr_tb_imp = tb_config['cmd_scr_imp_db_schema']+'"'+ cmd_scr_tb_imp_tb + '"'

    
    # get source and output table for export commodity
    cmd_scr_tb_exp_tb = tb_config['cmd_scr_tb_exp_tb']+ str(prv_yr_1[2:]) + str(run_yr[2:])
    cmd_scr_tb_exp = tb_config['cmd_scr_exp_db_schema']+'"'+ cmd_scr_tb_exp_tb + '"'

    # get source and output table for import country
    cntry_scr_tb_imp_tb = tb_config['cntry_scr_tb_imp_tb'] + str(prv_yr_1[2:]) + str(run_yr[2:])
    cntry_scr_tb_imp = tb_config['cntry_scr_imp_db_schema']+'"'+ cntry_scr_tb_imp_tb + '"'

    # get source and output table for export commodity
    cntry_scr_tb_exp_tb = tb_config['cntry_scr_tb_exp_tb']+ str(prv_yr_1[2:]) + str(run_yr[2:])
    cntry_scr_tb_exp = tb_config['cntry_scr_exp_db_schema']+'"'+ cntry_scr_tb_exp_tb + '"'


    prv_yr = get_year_format(run_yr, num)

    run_yr =  str(int(run_yr)-1)+ run_yr[2:]

    return {
        'run_yr': int(run_yr),
        'prv_yr': int(prv_yr),
        'pct_change_yr_compr':int(num),
        'cmd_scr_tb_exp':cmd_scr_tb_exp,
        'cmd_otpt_tb_exp':tb_config['cmd_otpt_tb_exp'],
        'cmd_scr_tb_imp':cmd_scr_tb_imp,
        'cmd_otpt_tb_imp':tb_config['cmd_otpt_tb_imp'],
        'cmd_int_exp_tb':tb_config['cmd_int_exp_tb'],
        'cmd_int_imp_tb':tb_config['cmd_int_imp_tb'],
        'cmd_trd_dft_tb':tb_config['cmd_trd_dft_tb'],
        'cntry_scr_tb_exp': cntry_scr_tb_exp,
        'cntry_scr_tb_imp': cntry_scr_tb_imp,
        'cntry_cd_mapping': tb_config['cntry_cd_mapping'],
        'cntry_otpt_tb_exp':tb_config['cntry_otpt_tb_exp'],
        'cntry_otpt_tb_imp':tb_config['cntry_otpt_tb_imp'],
        'cntry_int_exp_tb': tb_config['cntry_int_exp_tb'],
        'cntry_int_imp_tb': tb_config['cntry_int_imp_tb'],
        'cntry_trd_dft_tb': tb_config['cntry_trd_dft_tb']
    }