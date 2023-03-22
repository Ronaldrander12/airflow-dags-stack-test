import os
import logging
from io import BytesIO
from pathlib import Path
from datetime import date, datetime

import pandas as pd
from pandas import DataFrame
from google.cloud import storage
from google.cloud import bigquery
from google.cloud import firestore_v1

from data_functions.credentials_functions import Credentials

class Storage():
  def __init__(self):
    self.creds = Credentials.Get_Gcp_Creds()
    self.project_id = "algebraic-notch-380814"
    self.base_bucket = "datalake-stack-test"

  def Get_Client(self):
    client_storage = storage.Client(
      project= self.project_id,
      credentials= self.creds
    )

    return client_storage
  
  def Upload_File(self, df:DataFrame, zone:str, dataset:str, name_arq:str, format:str) -> None:
    client = self.Get_Client()
    bucket = client.get_bucket(f"{self.base_bucket}-{zone}")
    blob = bucket.blob(f"{dataset}/{name_arq}.{format}")

    if format == "json":  
      return blob.upload_from_string(df.to_json())
    
    elif format == "parquet":
      return blob.upload_from_string(df.to_parquet())
  
  def Download_File(self, zone:str, dataset:str, name_arq:str, format:str) -> DataFrame:
    client = self.Get_Client()
    bucket = client.get_bucket(f"{self.base_bucket}-{zone}")
    blob = bucket.blob(f"{dataset}/{name_arq}.{format}")
    content_bytes = BytesIO(blob.download_as_bytes())
    
    if format == "json":  
      df = pd.read_json(content_bytes)
      return df
    
    elif format == "parquet":
      df = pd.read_parquet(content_bytes)
      return df
    

class BigQuery:
  def __init__(self) -> None:
    self.creds = Credentials.Get_Gcp_Creds()
    self.project_id = "algebraic-notch-380814"

  def Insert_Registers(self, df:DataFrame, dataset_name:str, table_name:str, if_exists:str="append"):
    
    return df.to_gbq(
        destination_table=f"{dataset_name}.{table_name}", 
        project_id=self.project_id, 
        credentials=self.creds, 
        if_exists=if_exists
      )