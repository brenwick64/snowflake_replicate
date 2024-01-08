import os
import shutil
from dotenv import load_dotenv
from datetime import datetime
from snowflake.snowpark import Session
from modules.InfrastructureDeepCopy import InfrastructureDeepCopy
from modules.DataDeepCopy import DataDeepCopy
from modules.snowflake_extractor import SnowflakeExtractor
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

snowflake_objects = [
    'TABLE',
    'VIEW',
    'FILE_FORMAT',
    'PROCEDURE',
    'STREAM',
    'TASK',
    'STAGE'
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
    print(f'\nCopied files to archive folder: {destination_path}-{timestamp}')

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
    
    s = SnowflakeExtractor(session=session, database=os.getenv("SNOWFLAKE_DATABASE"), schema=os.getenv("SNOWFLAKE_SCHEMA"))
    
    """ Infrastructure Copy """
    print('\n """ Infrastructure Copy """ ')
    for obj in snowflake_objects:
        print(f'\n{obj}S\n')
        object_list = s.get_object_list(obj)
        print(f'Found {len(object_list)} {obj.lower()}s')

        for object_name in object_list:
            print(f'Extracting DDL for {obj}: {object_name}')
            object_ddl = s.get_object_ddl(obj, object_name)
            if object_ddl:
                with open(f'snowflake/infrastructure/{obj.lower()}s/{object_name}.sql', 'w') as f:
                    f.write(object_ddl)
                    f.close()
            else:
                print(f'No DDL found for {obj}: {object_name}')

    
    # """ Data Copy """
    # print('\n """ Data Copy """ ')
    # data_copy = DataDeepCopy(session=session)
    # data_copy.deep_copy()
    
    """ Archive Data & Infrastructure """
    copy_files(source_path='snowflake', destination_path=f'archive/{os.getenv("SNOWFLAKE_DATABASE").lower()}-{os.getenv("SNOWFLAKE_SCHEMA").lower()}')
    
    
    
    