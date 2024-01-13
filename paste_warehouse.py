import os
import pathlib
import json
import pandas as pd
from dotenv import load_dotenv
from snowflake.snowpark import Session
from modules.snowflake_paster import SnowflakePaster

# Load environment variables & local files
load_dotenv(override=True) 
with open('config.json') as f:
    manifest = json.load(f)['build_manifest']

if __name__ == '__main__':
    
    connection_parameters = {
    "account": os.getenv("SNOWFLAKE_PASTE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_PASTE_USERNAME"),
    "password": os.getenv("SNOWFLAKE_PASTE_PASSWORD"),
    "role": os.getenv("SNOWFLAKE_PASTE_ROLE"),
    "warehouse": os.getenv("SNOWFLAKE_PASTE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_PASTE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_PASTE_SCHEMA"),
    }
    
    try:
        session = Session.builder.configs(connection_parameters).create()
    except Exception as e:  
        print(f'Error: {e}')
        exit()
        
    p = SnowflakePaster(session=session, staging_dir=pathlib.Path(__file__).parent / 'staging')
    
        
for step in manifest:
    print(f'\n[{step["type"].upper()}] Processing: {step["name"]}\n')
    results_list = p.process_step(step=step)
    for result in results_list:
        print(f'    {result}')
