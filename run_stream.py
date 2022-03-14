# Databricks notebook source
# MAGIC %md
# MAGIC #### Load utility and streaming classes and methods

# COMMAND ----------

# MAGIC %run ./utility

# COMMAND ----------

# MAGIC %run ./streaming

# COMMAND ----------

# MAGIC %run ./notebook_job_result

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Run stream and create Spark SQL Table if it doesn't exist

# COMMAND ----------

import json
from datetime import datetime

# Set default output parameters
job_id = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
message = "finished successfully"
error_flag = False

try:
    # Get all widget parameters
    source_name = dbutils.widgets.get("source_name")
    mount_point = dbutils.widgets.get("mount_point")
    storage_account = dbutils.widgets.get("storage_account")
    base_path = dbutils.widgets.get("base_path")
    file_structure = dbutils.widgets.get("file_structure")

    table_name = dbutils.widgets.get("table_name")
    file_format = dbutils.widgets.get("file_format")
    trigger_once = dbutils.widgets.get("trigger_once")

    # get queue sas for specified source storage account
    queue_sas = get_queue_sas(storage_account)
    # get service principal used for accessing storage
    client_config = get_client_config()
    # create stream config by adding queue_sas to client_config
    stream_config = dict(client_config, queueSas=queue_sas)

    # Await stream termination. 
    await_termination = trigger_once
    # Stream load and output paths
    relative_path = file_structure.replace("table", table_name)
    load_path = f"{mount_point}/{base_path}/{relative_path}"
    base_output_path = "/mnt/oatley_delta_lake/delta_tables"
    output_path = f"{base_output_path}/{source_name}/{table_name}"

    # Table stream parameter dictionary used by DeltaStream
    table_parameters = {
        "name":table_name,
        "file_format":file_format,
        "trigger_once":trigger_once,
    }
    # Create and run DeltaStream until termination
    # This needs to be changed for streaming
    # table_parameters contains the kwargs to run the stream, originally loaded from table metadata
    DeltaStream(load_path, output_path, stream_config, 
                start_immediately=True, await_termination=await_termination,
                **table_parameters)


    # Create Spark SQL Table on top of Delta table if it doesn't exist
    database = "raw"
    sql_table_name = f"{source_name}__{table_name}"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    spark.sql(f"USE {database}")
    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS {sql_table_name} 
      USING DELTA
      LOCATION '{output_path}'
    """)
except Exception as e:
    error_flag = True
    message = f"Streaming generated an error: {str(e)}"
    print(f"An error occured: {str(e)}")
finally:
    notebook_job_result = {
        "datetime": str(datetime.utcnow()),
        "job_id": job_id, 
        "error_flag": True, 
        "message": message
    } #NotebookJobResult(job_id=job_id, error_flag=error_flag, message=message)
    dbutils.notebook.exit(notebook_job_result)
