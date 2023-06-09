import os
import base64
import json
from pathlib import Path

from google.oauth2 import service_account
from airflow.models.connection import Connection
from airflow.models import Variable

class Credentials():
    def __init__(self) -> None:
        self.conn_api = Connection().get_connection_from_secrets(conn_id="conn_api_creds")        

    def Get_Gcp_Creds(self):
        base_file_creds = Variable.get("CREDS_GCP")

        json_acct_info = json.loads(base_file_creds)
        
        access_creds = service_account.Credentials.from_service_account_info(json_acct_info)

        return access_creds    

    def Get_Api_Creds(self, conn_api_id:str) -> str:
        api_user = str(self.conn_api.login)
        api_pass = str(self.conn_api.get_password())

        base_format = f"{api_user}:{api_pass}"
        bytes_format = base_format.encode('ascii')
        base64_format = base64.b64encode(bytes_format).decode('ascii')

        return base64_format

    def Get_Db_Creds(self, conn_db_id:str, instancia_ref:str) -> dict:
        conn_db = Connection().get_connection_from_secrets(conn_id=conn_db_id)
        dict_conn = dict()
        dict_conn["conn_type"] = instancia_ref
        dict_conn["conn_host"] = str(conn_db.host)
        dict_conn["conn_login"] = str(conn_db.login)
        dict_conn["conn_password"] = str(conn_db.password)
        dict_conn["conn_database"] = str(conn_db.schema).split(".")[0]
        dict_conn["conn_schema"] = str(conn_db.schema).split(".")[1]
        dict_conn["conn_port"] = str(conn_db.port)
    
        return dict_conn