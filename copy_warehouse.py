import os
import shutil
import json
from dotenv import load_dotenv
from datetime import datetime
from snowflake.snowpark import Session
from modules.snowflake_copier import SnowflakeCopier

# Load environment variables & local files
load_dotenv() 
with open('config.json') as f:
    config = json.load(f)['config']
    
# Load local configuration settings
SNOWFLAKE_OBJECTS = config['snowflake_objects']
STAGING_DIRECTORY = config['staging_directory']
INFRASTRUCTURE_FOLDERS = config['infrastructure_folders']
DATA_FOLDERS = config['data_folders']
    
    
def setup_folders():
    # Creates infrastructure directory if it doesn't exist (replaces existing if it does)
    infra_path = STAGING_DIRECTORY + '/infrastructure'
    if not os.path.exists(infra_path):
        os.makedirs(infra_path)
    else:
        shutil.rmtree(infra_path)
        os.makedirs(infra_path)
        
    for folder in INFRASTRUCTURE_FOLDERS:
        os.makedirs(f'{infra_path}/{folder}')
     
    # Creates data directory if it doesn't exist (replaces existing if it does)   
    data_path = STAGING_DIRECTORY + '/data'
    if not os.path.exists(data_path):
        os.makedirs(data_path)
    else:
        shutil.rmtree(data_path)
        os.makedirs(data_path)
        
    for folder in DATA_FOLDERS:
        os.makedirs(f'{data_path}/{folder}')
    
def archive_warehouse(source_path, destination_path):
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    shutil.copytree(source_path, f'{destination_path}-{timestamp}')
    print(f'\nCopied files to archive folder: {destination_path}-{timestamp}')

if __name__ == '__main__':
    
    connection_parameters = {
    "account": os.getenv("SNOWFLAKE_COPY_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_COPY_USERNAME"),
    "password": os.getenv("SNOWFLAKE_COPY_PASSWORD"),
    "role": os.getenv("SNOWFLAKE_COPY_ROLE"),
    "warehouse": os.getenv("SNOWFLAKE_COPY_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_COPY_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_COPY_SCHEMA"),
    }
    
    try:
        session = Session.builder.configs(connection_parameters).create()
    except Exception as e:  
        print(f'Error: {e}')
        exit()
    
    setup_folders()
    
    s = SnowflakeCopier(session=session, database=os.getenv("SNOWFLAKE_DATABASE"), schema=os.getenv("SNOWFLAKE_SCHEMA"))
    
    # Infrastructure Deep Copy
    print('\n """ Infrastructure Copy """ ')
    
    # For each object type, get a list of objects in the Snowflake environment
    for obj in SNOWFLAKE_OBJECTS:
        print(f'\n{obj}S\n')
        object_list = s.get_object_list(obj)
        print(f'Found {len(object_list)} {obj.lower()}s')

        # For each object listed, extract the DDL and write it to a file
        for object_name in object_list:
            print(f'Extracting DDL for {obj}: {object_name}')
            object_ddl = s.get_object_ddl(obj, object_name)
            if object_ddl:
                with open(f'{STAGING_DIRECTORY}/infrastructure/{obj.lower()}s/{object_name}.sql', 'w') as f:
                    f.write(object_ddl)
                    f.close()
            else:
                print(f'No DDL found for {obj}: {object_name}')
                
    print('\n """ Data Copy """ ')
    
    # Extract and write all table data to csv files
    print('\nTABLES\n')
    table_list = s.get_object_list('TABLE')
    for table in table_list:
        try:
            s.save_table_data(table_name=table, target_directory=f'{STAGING_DIRECTORY}/data/tables/{table}')
            print(f'Extracted data for table: {table}')
        except Exception as e:
            print(f'Error: {e}')
            print(f'    Skipping Table: {table}')
            continue 
        
    # Extract and write all stage data to csv files
    print('\nSTAGES\n')
    stage_list = s.get_object_list('STAGE')
    for stage in stage_list:
        try:
            s.save_stage_data(stage_name=stage, target_directory=f'{STAGING_DIRECTORY}/data/stages/{stage}')
            print(f'Extracted data for stage: {stage}')
        except Exception as e:
            print(f'Error: {e}')
            print(f'    Skipping Stage: {stage}')
            continue
    
    
    """ Archive Data & Infrastructure """
    archive_warehouse(source_path=STAGING_DIRECTORY, destination_path=f'archive/{os.getenv("SNOWFLAKE_DATABASE").lower()}-{os.getenv("SNOWFLAKE_SCHEMA").lower()}')
    