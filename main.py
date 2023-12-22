import os
import shutil
from dotenv import load_dotenv
from snowflake.snowpark import Session
from modules.InfrastructureDeepCopy import InfrastructureDeepCopy
load_dotenv() 

infrastructure_folders = [
    "tables",
    "views",
    "file_formats",
    "procedures",
    "streams",
    "tasks",
    "stages"
]

data_folders = [
    "table_data",
    "stage_data"
]

object_configs = [
    {
        "label" : "TABLE",
        "plural": "TABLES",
        "field_name" : "TABLE_NAME",
        "conditions" : ["TABLE_SCHEMA = 'PUBLIC'", "TABLE_TYPE = 'BASE TABLE'"]
    },
    {
        "label" : "VIEW",
        "plural": "VIEWS",
        "field_name" : "TABLE_NAME",
        "conditions" : ["TABLE_SCHEMA = 'PUBLIC'"]
    },
    {
        "label" : "FILE_FORMAT",
        "plural": "FILE_FORMATS",
        "field_name" : "FILE_FORMAT_NAME",
        "conditions" : ["FILE_FORMAT_SCHEMA = 'PUBLIC'"]
    },
    {
        "label" : "PROCEDURE",
        "plural": "PROCEDURES",
        "field_name" : "PROCEDURE_NAME",
        "conditions" : ["PROCEDURE_SCHEMA = 'PUBLIC'"]
    },
    {
        "label" : "STREAM",
        "plural": "STREAMS",
        "field_name" : "name",
        "conditions" : []
    },
    {
        "label" : "TASK",
        "plural": "TASKS",
        "field_name" : "name",
        "conditions" : []
    },
    {
        "label" : "STAGE",
        "plural": "STAGES",
        "field_name" : "name",
        "conditions" : []
    }
]

def setup_folders():
    infra_path = 'infrastructure'
    if not os.path.exists(infra_path):
        os.makedirs(infra_path)
    else:
        shutil.rmtree(infra_path)
        os.makedirs(infra_path)
        
    for folder in infrastructure_folders:
        os.makedirs(f'{infra_path}/{folder}')
        
    data_path = 'data'
    if not os.path.exists(data_path):
        os.makedirs(data_path)
    else:
        shutil.rmtree(data_path)
        os.makedirs(data_path)
        
    for folder in data_folders:
        os.makedirs(f'{data_path}/{folder}')
    

if __name__ == '__main__':
    
    connection_parameters = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USERNAME"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    }
    session = Session.builder.configs(connection_parameters).create()
        
    setup_folders()
    infra = InfrastructureDeepCopy(session=session, configs_list=object_configs)
    infra.deep_copy()
    
    
    