import os
import json
import requests
from pathlib import Path
from datetime import datetime, date, timedelta

import requests
import pandas as pd
from pandas import DataFrame

from data_functions.credentials_functions import Credentials
from data_functions.storage_functions import Storage, BigQuery
from data_functions.database_functions import DataBase, FireBase

class IngestionApi():
  def __init__(self, dt_load:date=None):
    self.n_req = 10000
    self.base_url = "https://imunizacao-es.saude.gov.br/"
    self.creds = Credentials().Get_Api_Creds(conn_api_id='conn_api_creds')
    self.target_table = "api_sus"
    
    if dt_load:
      self.dt_load = dt_load
    else:
      self.dt_load = date.today() - timedelta(days=1)
    
    self.base_dataset = f"api_sus/{str(self.dt_load)}"
    self.base_arq = f"api_{str(self.dt_load)}"
    self.storage = Storage()

  def Load_Request(self) -> list:
    if self.n_req <= 10000:
      base_url = f"{self.base_url}_search"
      payload = json.dumps({
        "size": self.n_req
      })
      headers = {
        'Authorization': f'Basic {self.creds}',
        'Content-Type': 'application/json'
      }
      
      response = requests.request("POST", base_url, headers=headers, data=payload)
      payload = response.json().get('hits').get('hits')
      
    return payload
  
  def Load_Transient(self) -> None:    
    payload = self.Load_Request()
    df_load = pd.DataFrame(payload)
    df_addition = pd.DataFrame(list(df_load['_source']))
    df_load.drop(columns=['_source'], inplace=True)
    df_load = pd.concat([df_load, df_addition], axis=1)    

    content_full = list()
    for index, row in df_load.iterrows():
      dt_vaci = datetime.strptime(row["vacina_dataAplicacao"].split("T")[0], "%Y-%m-%d").date()

      if dt_vaci == self.dt_load:  
        content_full.append(list(row.values))
      
    df_filter = pd.DataFrame(content_full, columns=df_load.columns)

    return self.storage.Upload_File(
        df=df_filter,
        zone="transient",
        dataset=self.base_dataset,
        name_arq=self.base_arq,
        format="parquet"
    )

  def Read_Transient(self) -> DataFrame:

    return self.storage.Download_File(
      zone="transient",
      dataset=self.base_dataset,
      name_arq=self.base_arq,
      format="parquet"
    )

  def Load_Raw(self) -> None:
    df_load = self.Read_Transient()
    columns_add = ["_id", "paciente_idade", 
                   "paciente_enumSexoBiologico", "estabelecimento_uf", 
                   "estabelecimento_municipio_nome", "vacina_fabricante_nome", 
                   "vacina_descricao_dose", "vacina_dataAplicacao", 
                   "vacina_categoria_nome", "estalecimento_noFantasia"]
    
    df_load.drop(columns=[column for column in df_load.columns if column not in columns_add], inplace=True)
    df_load["vacina_dataAplicacao"] = [datetime.strptime(dt.split("T")[0], "%Y-%m-%d").date() for dt in df_load["vacina_dataAplicacao"]]     
    df_load.columns = [column.lower() for column in df_load.columns]

    return self.storage.Upload_File(
        df=df_load,
        zone="raw",
        dataset=self.base_dataset,
        name_arq=self.base_arq,
        format="parquet"
    )  

  def Read_Raw(self) -> DataFrame:
    
    return self.storage.Download_File(
      zone="raw",
      dataset=self.base_dataset,
      name_arq=self.base_arq,
      format="parquet"
    )

  def Load_Postgres(self) -> None:
    df_load = self.Read_Raw()

    database = DataBase(conn_db_id="conn_db_api")
    
    return database.Insert_Registers(
      df=df_load,
      table_name=self.target_table,
      if_exist="append",
      index=False
    )

  def Load_Firebase(self) -> None:
    df_load = self.Read_Raw()
    payload = df_load.to_dict(orient="records")
    
    for register in payload:
      register["vacina_dataaplicacao"] = str(register["vacina_dataaplicacao"])

    database = FireBase()

    return database.Insert_Documents(
      payload=payload,
      dt_ref=self.dt_load,
      collection_name=self.target_table
    )

  def Load_Bigquery(self) -> None:
    df_load = self.Read_Raw()
    df_load["vacina_dataaplicacao"] = [str(dt) for dt in list(df_load["vacina_dataaplicacao"])]

    bigquery = BigQuery()
  
    return bigquery.Insert_Registers(
      df=df_load,
      dataset_name="raw",
      table_name=self.target_table,
      if_exists="append"
    )

