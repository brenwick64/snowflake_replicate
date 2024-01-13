import os
from snowflake.snowpark import Session
import pandas as pd


class SnowflakePaster:

    def __init__(self, session: Session, staging_dir: str) -> None:
        self.session = session
        self.staging_dir = staging_dir
    
    
    def process_step(self, step: dict):
        return getattr(self, f'_SnowflakePaster__process_{step["type"]}')(session=self.session, step=step)
        
    
    def __process_infrastructure(self, session: Session, step: dict):
        results = []
        infrastructure_dir = f'{self.staging_dir}/{step["path"]}'
        for infrastructure_file in os.listdir(infrastructure_dir):
            with open(f'{infrastructure_dir}/{infrastructure_file}') as f:
                ddl = f.read()
                result = session.sql(ddl).collect()
                results.append(result[0][0])                
        return results  
                
                
    def __process_data(self, session: Session, step: dict):
    
        def process_stage_data():
            results = []
            stage_dir = f'{self.staging_dir}/{step["path"]}'
            for stage_folder in os.listdir(stage_dir):
                for stage_file in os.listdir(f'{stage_dir}/{stage_folder}'):
                    sql_statement = f'PUT file://{stage_dir}/{stage_folder}/{stage_file} @{stage_folder}'
                    result = session.sql(sql_statement).collect()
                    file_name = result[0][0]
                    file_status = result[0][6]
                    results.append(f'{file_name} - {file_status}')
                    
            return results
                    
        def process_table_data():
            results = []
            table_dir = f'{self.staging_dir}/{step["path"]}'
            for table_name in os.listdir(table_dir):
                pandas_df = pd.read_csv(f'{table_dir}/{table_name}')
                try:
                    result = session.write_pandas(df=pandas_df, table_name=table_name.replace('.csv', ''))
                    results.append(f'Successfully loaded {len(result.collect())} records into table: {table_name}')
                    
                except Exception as e:
                    result.append(f'Error: {e}')
                    
            return results
            
                    
        if step['name'] == 'STAGE':
            results = process_stage_data()
        elif step['name'] == 'TABLE':
            results = process_table_data()
            
        return results
                                
        
    #TODO
    def __process_command(self, session: Session, step: dict):
        pass
    

