# Databricks notebook source
import glob
import os

class DeltaStream():
    def __init__(self, 
                 load_path, output_path, storage_connection_config, 
                 start_immediately=False, await_termination=False, 
                 name=None, file_format=None, trigger_once=True, partition_columns=None, schema_path=None):
        if file_format.lower() not in ["parquet", "csv", "json"]:
            raise Exception("A valid file_format needs to be specified.")
        
        self.load_path = load_path
        self.file_format = file_format
        self.output_path = output_path
        self.storage_connection_config = storage_connection_config
        self.schema_path = schema_path
        self.trigger_once = trigger_once
        self.await_termination = await_termination
        self.name = output_path if name is None else name
        
        # Delta table checkpoint information is saved in the delta table folder
        self.checkpoint_location = f"{output_path}/_checkpoint"
        self.infer_schema = schema_path is None and file_format != "parquet"
        
        self.stream_df = self._initialise()
        self.streaming_query = None
        
        if start_immediately:
            self.start()
        
    def _initialise(self):
        
        #### FIX NON DBFS FILE PATH. Might want to replace glob.
        # Get the list of files to read sorted by order. If empty, raise error. Makes sure path is readable for glob.glob
        load_file_list = "/dbfs"+ self.load_path.replace("dbfs:", "").replace("/dbfs", "")
        load_file_list = glob.glob(load_file_list)
        if len(load_file_list) == 0:
            raise Exception(f"No files were found at {self.load_path}.") ### Change exception type
        #print("Found files: ", load_file_list)
        
        # Automatically infer schema for non-parquet source unless schema is supplied 
        if not self.infer_schema:
            # read schema from most recent file for parquet
            if self.file_format == "parquet":
                # Transforms path back to format for spark.read
                schema_load_file = sorted(load_file_list, key=os.path.getmtime, reverse=True)[0].replace("/dbfs", "dbfs:")
                schema = spark.read.parquet(schema_load_file).schema
            if self.file_format != "parquet":
                schema = None
                raise Exception("Schema read from file not implemented for non-parquet.")
        # Set max number of file to load for schema inference
        # spark.conf.set("spark.databricks.cloudFiles.schemaInference.sampleSize.numBytes", 10gb"")

        # Define autoloader options
        cloudfiles = {
            "cloudfiles.format": self.file_format,        
            "cloudfiles.subscriptionId": self.storage_connection_config["subscriptionId"],
            "cloudfiles.connectionString": self.storage_connection_config["queueSas"],
            "cloudfiles.tenantId": self.storage_connection_config["tenantId"],
            "cloudfiles.clientId": self.storage_connection_config["clientId"],
            "cloudfiles.clientSecret": self.storage_connection_config["clientSecret"],
            "cloudfiles.resourceGroup": self.storage_connection_config["resourceGroup"],
            "cloudFiles.useNotifications": "true",
            #"cloudFiles.partitionColumns":"",
        }
        
        if self.infer_schema:
            cloudfiles.update({
                "cloudFiles.schemaLocation": self.checkpoint_location,
                "cloudFiles.inferColumnTypes": "true",
                "cloudFiles.schemaEvolutionMode":"addNewColumns",
            })

        # Define the read source for the stream
        if self.infer_schema:
            stream_df = (spark.readStream
                .format("cloudfiles")
                .options(**cloudfiles)
                .load(self.load_path))   
        else:
            stream_df = (spark.readStream
                .schema(schema)
                .format("cloudfiles")
                .options(**cloudfiles)
                .load(self.load_path))   
        
        return stream_df
        
    def start(self):        
        write_stream_options = {
            "checkpointLocation": self.checkpoint_location, 
            "mergeSchema": "true"
        }
    
        # Set the write stream options 
        stream_writer = (self.stream_df.writeStream
               .format("delta")
               .outputMode("append")
               .queryName(f"AutoLoad_{self.name.capitalize()}")
               .options(**write_stream_options))
        
        if self.trigger_once:
            stream_writer.trigger(once = True)
    
        self.streaming_query = stream_writer.start(self.output_path)
    
        if self.await_termination:
            if not self.trigger_once:
                raise Exception("Trigger once await_termination=True only for trigger_once=True.")
            self.streaming_query.awaitTermination()
        
    def active(self):
        if self.streaming_query is None:
            return False
        else:
            return self.streaming_query.isActive

    def status(self):
        if self.streaming_query is None:
            return "Not started"
        else:
            return self.streaming_query.status
        
    def last_progress(self):
        if self.streaming_query is None:
            return "Not started"
        else:
            return self.streaming_query.lastProgress
