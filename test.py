import os
from dotenv import load_dotenv
from snowflake.snowpark import Session
load_dotenv() 

def connect():
    connection_parameters = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USERNAME"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    }

    return Session.builder.configs(connection_parameters).create()

def get_all_stages(session: Session):
    with session.query_history() as query_history:
        session.sql(f"SHOW STAGES;").collect()
        query_id = "'" + query_history.queries[0][0] + "'"
        obj_list = session.sql(f'SELECT "name" FROM table(result_scan({query_id}))').to_pandas().iloc[:, 0].values.flatten().tolist()
        return obj_list
    
def get_stage_files(session: Session, stage_name: str):
    with session.query_history() as query_history:
        session.sql(f"LIST @{stage_name}").collect()
        query_id = "'" + query_history.queries[0][0] + "'"
        obj_list = session.sql(f'SELECT "name" FROM table(result_scan({query_id}))').to_pandas().iloc[:, 0].values.flatten().tolist()
        return obj_list
    
def get_all_tables(session: Session):
    obj_df = session.sql(f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'PUBLIC' AND TABLE_TYPE = 'BASE TABLE'; ").to_pandas()
    obj_list = obj_df.iloc[:, 0].values.flatten().tolist()
    return obj_list


def get_table_data(session: Session, table_name: str):  
    session.sql(f"SELECT * FROM {table_name};").to_pandas().to_csv(f"data/table_data/{table_name}.csv")
          


if __name__ == "__main__":
    session = connect()
    
    # Copy stage data
    for stage in get_all_stages(session):
        session.file.get(f"@{stage.lower()}", f"data/stage_data/{stage}")
        
    # Copy table data
    for table in get_all_tables(session):
        get_table_data(session, table)