from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

default_args = {
    "owner": "Stack - Engenharia de Dados",
    "start_date": days_ago(1)
}

default_requirements = ["pandas", "sqlalchemy", "pyodbc", "pyarrow", "fastparquet", "requests", "google-cloud-storage", "google-cloud-bigquery","google-auth", "gcsfs", "google-cloud-firestore", "pandas-gbq"]

@dag(dag_id="Dag_Ingestion_Api_Sus", schedule_interval="0 6 * * *", default_args=default_args)
def Dag_Ingestion_Api_Sus():

  @task.virtualenv(task_id="Load_DataLake_Transient", requirements=default_requirements, system_site_packages=True)
  def Load_DataLake_Transient():
    from api_sus.process_ingestion_api import IngestionApi
    
    main = IngestionApi()
    main.Load_Transient()

  @task.virtualenv(task_id="Load_DataLake_Raw", requirements=default_requirements, system_site_packages=True)
  def Load_DataLake_Raw():
    from api_sus.process_ingestion_api import IngestionApi
    
    main = IngestionApi()
    main.Load_Raw()

  @task.virtualenv(task_id="Load_Database_Postgres", requirements=default_requirements, system_site_packages=True)
  def Load_Database_Postgres():
    from api_sus.process_ingestion_api import IngestionApi
    
    main = IngestionApi()
    main.Load_Postgres()

  @task.virtualenv(task_id="Load_Database_Firebase", requirements=default_requirements, system_site_packages=True)
  def Load_Database_Firebase():
    from api_sus.process_ingestion_api import IngestionApi
    
    main = IngestionApi()
    main.Load_Firebase()

  @task.virtualenv(task_id="Load_Bigquery", requirements=default_requirements, system_site_packages=True)
  def Load_Bigquery():
    from api_sus.process_ingestion_api import IngestionApi
    
    main = IngestionApi()
    main.Load_Bigquery()

  Process_Load_Transient_Task = Load_DataLake_Transient()
  Process_Load_Raw_Task = Load_DataLake_Raw()
  Process_Load_Postgres_Task = Load_Database_Postgres()
  Process_Load_Firebase_Task = Load_Database_Firebase()
  Process_Load_Bigquery_Task = Load_Bigquery()

  Process_Load_Transient_Task >> Process_Load_Raw_Task >> Process_Load_Postgres_Task
  Process_Load_Transient_Task >> Process_Load_Raw_Task >> Process_Load_Firebase_Task
  Process_Load_Transient_Task >> Process_Load_Raw_Task >> Process_Load_Bigquery_Task


dag = Dag_Ingestion_Api_Sus()