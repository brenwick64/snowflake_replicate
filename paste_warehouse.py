import os
import pathlib
import json
import pandas as pd
from dotenv import load_dotenv
from snowflake.snowpark import Session

# Load environment variables & local files
load_dotenv() 
with open('config.json') as f:
    manifest = json.load(f)['build_manifest']
    

def handle_infrastructure(session: Session, step: dict):
    print(f'\n[Infrastructure] - Building Snowflake Object(s) - {step["name"]}\n')
    infrastructure_folder = step['path']
    for infrastructure_file in os.listdir(infrastructure_folder):
        with open(f'{infrastructure_folder}/{infrastructure_file}') as f:
            ddl = f.read()
            result = session.sql(ddl).collect()
            print(f'    {result[0][0]}')

    
def handle_data(session: Session, step: dict):
    
    def parse_stage_result(result):
        file_name = result[0][0]
        file_status = result[0][6]
        return f'{file_name} - {file_status}'
    
    print(f'\n[Data] - Inserting {step["name"]} Data\n')
    if step['name'] == 'STAGE':
        parent_dir = pathlib.Path(__file__).parent / f'staging\data\stages\\'
        stage_folders = step['path']
        for stage_folder in os.listdir(stage_folders):
            stage_files = os.listdir(f'{stage_folders}/{stage_folder}')
            for f in stage_files:
                statement = f'PUT file://{parent_dir}\{stage_folder}\{f} @{stage_folder}'
                parsed_result = parse_stage_result(result=session.sql(statement).collect())
                print(f'    {parsed_result}')
                
    elif step['name'] == 'TABLE':
        parent_dir = pathlib.Path(__file__).parent / f'staging\data\\tables\\'
        for table_name in os.listdir(step['path']):
            file_path = f'{parent_dir}\{table_name}'
            pandas_df = pd.read_csv(file_path)
            try:
                result = session.write_pandas(df=pandas_df, table_name=table_name.replace('.csv', ''))
                print(f'    Successfully loaded {len(result.collect())} records into table: {table_name}')
                
            except Exception as e:
                print(f'Error: {e}')
                continue                       
        
    
#TODO;
def handle_command(session: Session, step: dict):
    pass
    # print('Handling command')
    


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
        
for step in manifest:
    locals()[f'handle_{step["type"]}'](session=session, step=step)

