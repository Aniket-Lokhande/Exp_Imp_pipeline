from base_runner import base_runner
from typing import Optional
import psycopg2
from psycopg2.extensions import connection
from pathlib import Path
from utils import PROJECT_ROOT, load_conn_params

class country_runner(base_runner):
    SQL_SRC = Path(PROJECT_ROOT)/'sqls'

    def __init__(self, conn: Optional[connection]=None):
        super(country_runner, self).__init__(conn)
        self.category = 'country'

    def get_config(self):
        return load_conn_params()