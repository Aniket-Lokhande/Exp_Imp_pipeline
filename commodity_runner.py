import sys
import os
from typing import Optional, Dict, Any
from base_runner import base_runner
import psycopg2
from psycopg2.extensions import connection
from pathlib import Path
from utils import PROJECT_ROOT, load_conn_params

class commodity_runner(base_runner):
    SQL_SRC = Path(PROJECT_ROOT) / "sqls"

    def __init__(self, conn: Optional[connection]=None) -> None:
        super(commodity_runner, self).__init__(conn)
        self.category = 'commodity'
    
    def get_config(self):
        return load_conn_params()