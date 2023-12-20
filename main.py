import os
from dotenv import load_dotenv
from snowflake.snowpark import Session
load_dotenv() 


primary_config = [
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
    }
]

secondary_config = [
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


def get_primary_objects_list(session: Session, config: dict):
    obj_df = session.sql(f"SELECT {config['field_name']} FROM INFORMATION_SCHEMA.{config['plural']} WHERE 1=1 AND {' AND '.join(config['conditions'])}").to_pandas()
    obj_list = obj_df.iloc[:, 0].values.flatten().tolist()
    return obj_list


def get_secondary_objects_list(session: Session, config: dict):
    with session.query_history() as query_history:
        session.sql(f"SHOW {config['plural']};").collect()
        query_id = "'" + query_history.queries[0][0] + "'"
        obj_list = session.sql(f'SELECT "{config["field_name"]}" FROM table(result_scan({query_id}))').to_pandas().iloc[:, 0].values.flatten().tolist()
        return obj_list
  
def get_primary_object_ddl(session: Session, obj_list: list, config: dict):
    ddl_list = []
    for obj in obj_list:
        print(f'        Copying: {config["label"]} {obj}')
        ddl = session.sql(f"SELECT GET_DDL('{config['label']}', '{obj}{'()' if config['label'] == 'PROCEDURE' else ''}')").to_pandas().iloc[:, 0].values.flatten()[0]
        ddl_list.append({ "name" : obj, "ddl" : ddl })
    return ddl_list

def get_stream_ddl(session: Session, obj_list: list, config: dict):
    ddl_list = []
    for obj in obj_list:
        print(f'        Copying: {config["label"]} {obj}')
        with session.query_history() as query_history:
            session.sql(f"DESC {config['label']} {obj}").collect()
            query_id = "'" + query_history.queries[0][0] + "'"
            desc_df = session.sql(f'SELECT "name" as stream_name, "table_name" FROM table(result_scan({query_id}))').to_pandas()   
            
            # Construct DDL         
            ddl_statement = f'CREATE OR REPLACE STREAM {desc_df.iloc[:, 0].values[0]} ON TABLE {desc_df.iloc[:, 1].values[0]};'
            ddl_list.append({ "name" : obj, "ddl" : ddl_statement })
            
    return ddl_list

def get_task_ddl(session: Session, obj_list: list, config: dict):
    ddl_list = []
    for obj in obj_list:
        print(f'        Copying: {config["label"]} {obj}')
        with session.query_history() as query_history:
            session.sql(f"DESC {config['label']} {obj}").collect()
            query_id = "'" + query_history.queries[0][0] + "'"
            desc_df = session.sql(f'SELECT "name", "warehouse", "schedule", "condition", "definition" FROM table(result_scan({query_id}))').to_pandas()  
            
            # Construct DDL
            ddl_statement = f"CREATE OR REPLACE TASK {desc_df.iloc[:, 0].values[0]} \n \
                                WAREHOUSE = {desc_df.iloc[:, 1].values[0]} \n \
                                SCHEDULE = '{desc_df.iloc[:, 2].values[0]}' \n \
                                WHEN {desc_df.iloc[:, 3].values[0]} \n \
                                AS {desc_df.iloc[:, 4].values[0]};" 
                                
        
            ddl_list.append({ "name" : obj, "ddl" : ddl_statement })
            
    return ddl_list

def get_stage_ddl(session: Session, obj_list: list, config: dict):
    ddl_list = []
    for obj in obj_list:
        print(f'        Copying: {config["label"]} {obj}')
        
        # Construct DDL
        ddl_statement = f"CREATE OR REPLACE STAGE {obj};"
        ddl_list.append({ "name" : obj, "ddl" : ddl_statement })
    return ddl_list
   
    
def write_ddl_to_file(ddl_list: dict, config: dict):
    for ddl_entry in ddl_list:
        with open(f'infrastructure/{config["plural"].lower()}/{ddl_entry["name"]}.sql', 'w') as f:
            f.write(ddl_entry["ddl"])
            
            

if __name__ == '__main__':
    session = connect()
    print('\n\nExtracting Primary Objects')
    for config in primary_config:
        print(f'\n    Working: {config["plural"]}')
        obj_list = get_primary_objects_list(session=session, config=config)
        obj_ddl_list = get_primary_object_ddl(session=session, obj_list=obj_list, config=config)
        write_ddl_to_file(ddl_list=obj_ddl_list, config=config)
    
    print('\n\nExtracting Secondary Objects')
    for config in secondary_config:
        print(f'\n    Working: {config["plural"]}')
        obj_list = get_secondary_objects_list(session=session, config=config)
        
        # Instead of calling get_primary_object_ddl, we call the function dynamically based on the config label to handle the different DDL construction
        obj_ddl_list = locals()[f"get_{config['label'].lower()}_ddl"](session=session, obj_list=obj_list, config=config)
        write_ddl_to_file(ddl_list=obj_ddl_list, config=config)

    


    
        
    

    
    
        