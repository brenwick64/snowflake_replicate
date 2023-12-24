import os
import shutil
from dotenv import load_dotenv
from datetime import datetime
from snowflake.snowpark import Session
from modules.InfrastructureDeepCopy import InfrastructureDeepCopy
from modules.DataDeepCopy import DataDeepCopy
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
        "conditions" : [f"TABLE_SCHEMA = '{os.getenv('SNOWFLAKE_SCHEMA')}'", "TABLE_TYPE = 'BASE TABLE'"]
    },
    {
        "label" : "VIEW",
        "plural": "VIEWS",
        "field_name" : "TABLE_NAME",
        "conditions" : [f"TABLE_SCHEMA = '{os.getenv('SNOWFLAKE_SCHEMA')}'"]
    },
    {
        "label" : "FILE_FORMAT",
        "plural": "FILE_FORMATS",
        "field_name" : "FILE_FORMAT_NAME",
        "conditions" : [f"FILE_FORMAT_SCHEMA = '{os.getenv('SNOWFLAKE_SCHEMA')}'", "FILE_FORMAT_NAME NOT LIKE '%temp%'"]
    },
    {
        "label" : "PROCEDURE",
        "plural": "PROCEDURES",
        "field_name" : "PROCEDURE_NAME",
        "conditions" : [f"PROCEDURE_SCHEMA = '{os.getenv('SNOWFLAKE_SCHEMA')}'"]
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
    infra_path = 'snowflake/infrastructure'
    if not os.path.exists(infra_path):
        os.makedirs(infra_path)
    else:
        shutil.rmtree(infra_path)
        os.makedirs(infra_path)
        
    for folder in infrastructure_folders:
        os.makedirs(f'{infra_path}/{folder}')
        
    data_path = 'snowflake/data'
    if not os.path.exists(data_path):
        os.makedirs(data_path)
    else:
        shutil.rmtree(data_path)
        os.makedirs(data_path)
        
    for folder in data_folders:
        os.makedirs(f'{data_path}/{folder}')
    
def copy_files(source_path, destination_path):
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    shutil.copytree(source_path, f'{destination_path}-{timestamp}')
    print(f'Copied files to archive folder: {destination_path}-{timestamp}')

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
    
    try:
        session = Session.builder.configs(connection_parameters).create()
    except Exception as e:  
        print(f'Error: {e}')
        exit()
    
    setup_folders()
    
    """ Infrastructure  Copy """
    print('\n """ Infrastructure  Copy """ ')
    infra_copy = InfrastructureDeepCopy(session=session, configs_list=object_configs)
    infra_copy.deep_copy()
    
    """ Data Copy """
    print('\n """ Data Copy """ ')
    data_copy = DataDeepCopy(session=session)
    data_copy.deep_copy()
    
    """ Archive Data & Infrastructure """
    copy_files(source_path='snowflake', destination_path=f'archive/{os.getenv("SNOWFLAKE_DATABASE").lower()}-{os.getenv("SNOWFLAKE_SCHEMA").lower()}')