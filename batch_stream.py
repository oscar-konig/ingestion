# Databricks notebook source
# MAGIC %md
# MAGIC ##### Load helper functions and objects

# COMMAND ----------

# MAGIC %run ./utility

# COMMAND ----------

# MAGIC %run ./pipeline_util

# COMMAND ----------

# MAGIC %run ./notebook_job_result

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Load Source and Table objects

# COMMAND ----------

yaml_path = '/Workspace/Repos/oscar.konig@dataedge.se/deltalake_ingest/tables_ingest/test_tables.yml'
sources, tables = load_sources_and_tables_from_yaml(yaml_path)

# iterate over sources and make sure they are mounted

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Run concurrent notebooks to load tables

# COMMAND ----------

from concurrent import futures
import time
from datetime import datetime
import ast

def run_with_retry(path, timeout_seconds, max_retries, arguments={}):
    num_retries = 0
    
    while True:
        try:
            notebook_job_result = dbutils.notebook.run(path, timeout_seconds, arguments)
            
            # LOG SUCCESS?
            return notebook_job_result
        except Exception as e:
            # LOG ERROR?
            if num_retries >= max_retries:
                message = f"Running notebook generated an error: {str(e)}"
                return {
                    "datetime": str(datetime.utcnow()),
                    "job_id": -1, 
                    "error_flag": True, 
                    "message": message
                }
            else:    
                print("Retrying error", e)
                num_retries += 1

def run_notebook_stream(table_parameters, source_parameters, max_retries=2, timeout_seconds=100000000):
    # rename table/source name field
    table_parameters = table_parameters.copy()
    table_parameters["table_name"] = table_parameters.pop("name")
    source_parameters = source_parameters.copy()
    source_parameters["source_name"] = source_parameters.pop("name")
    
    arguments = {**table_parameters, **source_parameters}
    
    result = run_with_retry(path = "run_stream",
                            timeout_seconds=timeout_seconds,
                            arguments=arguments,
                            max_retries=max_retries)
    return result

def batch_load(metadata_dict, max_concurrent=8):
    with futures.ThreadPoolExecutor(max_workers=max_concurrent) as executor:
        results = {}
        
        # submit all streams
        for source_dict in metadata["sources"]:
            # Mount storage detailed in source metadata if not mounted already
            mount_storage(source_dict["container"], source_dict["storage_account"], source_dict["mount_point"])   
            # Get the list of tables for current source. The tables list is removed from the source dictionary.
            table_dict_list = source_dict.pop("tables")
            
            for table_dict in table_dict_list:
                # run notebook in thread and get result
                max_retries = 2
                # Future object representing the execution of the callable, when all threads are finished we get the results
                future = executor.submit(run_notebook_stream, table_dict, source_dict, max_retries)
                
                # add metadata/load parameters to results dictionary, and temporarily add the future to get results later
                results[f"{source_dict['name']}__{table_dict['name']}"] = {
                    "table_dict": table_dict,
                    "source_dict": source_dict,
                    "future": future
                }
                
        # Extract run results return from notebook runs in futures
        # This has to be done outside of the submit loop 
        # since future.result() waits for the execution to finish
        for result in results.values():
            # remove future from result dictionary
            future = result.pop("future")
            # parse future result string
            notebook_job_result = ast.literal_eval(future.result())
            # add notebook run result to result dictionary
            result.update(notebook_job_result)
    
    return results

# COMMAND ----------

yaml_path = "/Workspace/Repos/oscar.konig@dataedge.se/ingestion/metadata.yaml"
metadata = read_metadata(yaml_path)

results = batch_load(metadata)


# COMMAND ----------

str(datetime.now())

# COMMAND ----------

results["dynamics_test__Ledger_ms"]
