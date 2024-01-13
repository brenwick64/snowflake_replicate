from snowflake.snowpark import Session
import pandas as pd

class SnowflakeCopier:
    
    def __init__(self, session: Session, database: str, schema: str) -> None:
        self.session = session
        self.database = database
        self.schema = schema
        self.sql_mappings = {
            "TABLE" : {
                "list_query" : f'SHOW TABLES;',
                "list_field_name" : f'"name"',
                "list_conditions" : [f" \"database_name\" = '{self.database}'", f" \"schema_name\" = '{self.schema}' "],
                "ddl_query" : f" SELECT GET_DDL('TABLE', '{self.database}.{self.schema}.<NAME>') "
            },
            
            "VIEW" : {
                "list_query" : f'SHOW VIEWS;',
                "list_field_name" : f'"name"',
                "list_conditions" : [f" \"database_name\" = '{self.database}'", f" \"schema_name\" = '{self.schema}' "],
                "ddl_query" : f" SELECT GET_DDL('VIEW', '{self.database}.{self.schema}.<NAME>') "
            },
            
            "FILE_FORMAT" : {
                "list_query" : f'SELECT * FROM INFORMATION_SCHEMA.FILE_FORMATS;',
                "list_field_name" : f'"FILE_FORMAT_NAME"',
                "list_conditions" : [f" \"FILE_FORMAT_CATALOG\" = '{self.database}'", f" \"FILE_FORMAT_SCHEMA\" = '{self.schema}' "],
                "ddl_query" : f" SELECT GET_DDL('FILE_FORMAT', '{self.database}.{self.schema}.<NAME>') "
            },
            
            # TODO: Add support for procedures with arguments
            "PROCEDURE" : {
                "list_query" : f'SHOW PROCEDURES;',
                "list_field_name" : f'CONCAT("name", \'()\') as name',
                "list_conditions" : [f" \"schema_name\" = '{self.schema}' ", f" \"description\" = 'user-defined procedure' "],
                "ddl_query" : f" SELECT GET_DDL('PROCEDURE', '{self.database}.{self.schema}.<NAME>') "
            },
            
            "STREAM" : {
                "list_query" : f'SHOW STREAMS;',
                "list_field_name" : f'"name"',
                "list_conditions" : [f" \"database_name\" = '{self.database}'", f" \"schema_name\" = '{self.schema}' "],
                "ddl_query" : f" SELECT GET_DDL('STREAM', '{self.database}.{self.schema}.<NAME>') "
            },
            
            "TASK" : {
                "list_query" : f'SHOW TASKS;',
                "list_field_name" : f'"name"',
                "list_conditions" : [f" \"database_name\" = '{self.database}'", f" \"schema_name\" = '{self.schema}' "],
                "ddl_query" : f" SELECT GET_DDL('TASK', '{self.database}.{self.schema}.<NAME>') "
            },
            
            "STAGE" : {
                "list_query" : f'SHOW STAGES;',
                "list_field_name" : f'"name"',
                "list_conditions" : [f" \"database_name\" = '{self.database}'", f" \"schema_name\" = '{self.schema}' "],
                "ddl_query" : f"SELECT 'CREATE OR REPLACE STAGE {self.database}.{self.schema}.<NAME>;' "
            }
        }
        
        
    def get_object_list(self, snowflake_object_type: str) -> list:
        with self.session.query_history() as query_history:
            
            # Extracts primary query strategy from list_sql_mappings
            self.session.sql(self.sql_mappings[snowflake_object_type]["list_query"]).collect()
            
            # Caches the query ID in order to filter all queries (including SHOW)
            query_id = "'" + query_history.queries[0][0] + "'"                           
                                             
            # Runs a traditional SELECT operation on any primary query                                
            object_list = self.session.sql(f'SELECT {self.sql_mappings[snowflake_object_type]["list_field_name"]} \
                                             FROM table(result_scan({query_id})) \
                                             WHERE 1=1 AND {" AND ".join(self.sql_mappings[snowflake_object_type]["list_conditions"])}'
                                            ).to_pandas().iloc[:, 0].values.flatten().tolist()
            
            
            return object_list
        
        
    def get_object_ddl(self, snowflake_object_type: str, object_name: str) -> str:
        with self.session.query_history() as query_history:
            
            # Extracts primary query strategy from ddl_sql_mappings
            self.session.sql(self.sql_mappings[snowflake_object_type]["ddl_query"].replace("<NAME>", object_name)).collect()
            
            # Caches the query ID in order to filter all queries
            query_id = "'" + query_history.queries[0][0] + "'"
            
            # Runs a traditional SELECT operation on any primary query and extracts the DDL from the first row
            object_ddl = self.session.sql(f'SELECT * FROM table(result_scan({query_id}))').to_pandas().iloc[:, 0].values.flatten()[0]
            
            return object_ddl
        
        
    def save_table_data(self, table_name: str, target_directory) -> pd.DataFrame:
        df = self.session.sql(f"SELECT * FROM {table_name};").to_pandas()
        try:
            df.to_csv(f'{target_directory}.csv', index=False)
        except Exception as e:
            raise e        
    
    
    def save_stage_data(self, stage_name: str, target_directory: str) -> None:
        try:
            self.session.file.get(f'@{stage_name.lower()}', f'{target_directory}')
        except Exception as e:
            raise e
