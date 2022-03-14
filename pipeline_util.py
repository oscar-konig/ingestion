# Databricks notebook source
import yaml 
import json

# Add asserts for keys and default values for robustness. Right now, Source and Table mostly extend dictionaries
# Maybe we should not access the dicts directly...
class Source:
    def __init__(self, source_dict):
        self.dict = source_dict

class Table:
    def __init__(self, source, table_dict):
        self.dict = table_dict
        self.source = source

def read_metadata(yaml_path, file_format="yaml"):
    with open(yaml_path) as file:
        if file_format == "yaml":
            metadata_dict = yaml.load(file, Loader=yaml.FullLoader)
        elif file_format == "json":
            metadata_dict = json.loads(file)
        else:
            raise Exception("Invalid metadata file format.")
            
    return metadata_dict
        
def load_sources_and_tables_from_yaml(yaml_path):
    metadata_dict = read_metadata(yaml_path)
    
    sources = []
    tables = []
    
    for source_dict in metadata_dict["sources"]:
        source_table_dicts = source_dict.pop("tables")
        source = Source(source_dict)
        sources.append(source)
        
        for table_dict in source_table_dicts:
            table = Table(source, table_dict)
            tables.append(table)
    
    return sources, tables
