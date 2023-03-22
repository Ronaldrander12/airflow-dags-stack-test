import sqlalchemy
import pandas as pd
from datetime import date, datetime
from pandas import DataFrame
from urllib.parse import quote_plus
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from google.cloud import firestore_v1

from data_functions.credentials_functions import Credentials

class DataBase():
        def __init__(self, conn_db_id:str, instancia_ref:str) -> None:
            self.conn_db_id = conn_db_id
            self.instancia_ref = instancia_ref
            self.creds_db = Credentials().Get_Db_Creds(conn_db_id=conn_db_id, instancia_ref=instancia_ref)
            self.engine = self.Get_Engine()
            
        def Get_Engine(self):
        
            if self.instancia_ref == "postgres":
                engine = create_engine(
                    url="postgresql+psycopg2://{}:{}@{}/{}".format(
                        self.creds_db.get('conn_login'),
                        self.creds_db.get('conn_password'),
                        self.creds_db.get('conn_host'),
                        self.creds_db.get('conn_database')
                    )
                )

            return engine
        
        def Insert_Registers(self, df:DataFrame, table_name:str, if_exist:str="append", index:bool=False):
             
             return df.to_sql(
                  name=table_name,
                  con=self.engine,
                  if_exists=if_exist,
                  index=index
             )
        
class FireBase():
  def __init__(self) -> None:
    self.creds = Credentials.Get_Gcp_Creds()
    self.project_id = "algebraic-notch-380814"
    self.client = self.Get_Client()

  def Get_Client(self):
    client = firestore_v1.Client(project=self.project_id, credentials=self.creds)
  
  def Insert_Documents(self, payload:list, dt_ref:date, collection_name:str):
    collection = self.client.collection(collection_name)

    for register in payload:
      document_id = f"{str(dt_ref)}_{register.get('_id')}"
      document_content = register

      collection.document(document_id=document_id).set(document_content)