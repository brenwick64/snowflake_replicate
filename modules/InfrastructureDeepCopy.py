from snowflake.snowpark import Session

class InfrastructureDeepCopy:
    def __init__(self, session: Session, configs_list: list):
        self.session = session
        self.configs_list = configs_list
        self.list_function_mappings = {
            "table" : self.__get_primary_objects_list,
            "view" : self.__get_primary_objects_list,
            "file_format" : self.__get_primary_objects_list,
            "procedure" : self.__get_primary_objects_list,
            "stream" : self.__get_secondary_objects_list,
            "task" : self.__get_secondary_objects_list,
            "stage" : self.__get_secondary_objects_list
        }
        self.ddl_function_mappings = {
            "table" : self.__get_primary_object_ddl,
            "view" : self.__get_primary_object_ddl,
            "file_format" : self.__get_primary_object_ddl,
            "procedure" : self.__get_primary_object_ddl,
            "stream" : self.__get_stream_ddl,
            "task" : self.__get_task_ddl,
            "stage" : self.__get_stage_ddl
        }
        
    
    def __get_primary_objects_list(self, config: dict):
        obj_df = self.session.sql(f"SELECT {config['field_name']} \
                                    FROM INFORMATION_SCHEMA.{config['plural']} \
                                    WHERE 1=1 AND {' AND '.join(config['conditions'])}").to_pandas()
        obj_list = obj_df.iloc[:, 0].values.flatten().tolist()
        return obj_list
    

    def __get_secondary_objects_list(self, config: dict):
        with self.session.query_history() as query_history:
            self.session.sql(f"SHOW {config['plural']};").collect()
            query_id = "'" + query_history.queries[0][0] + "'"
            obj_list = self.session.sql(f'SELECT "{config["field_name"]}" FROM table(result_scan({query_id}))').to_pandas().iloc[:, 0].values.flatten().tolist()
            return obj_list
    
    def __get_primary_object_ddl(self, obj: dict, config: dict):
        print(f'        Copying: {config["label"]} {obj}')
        ddl = self.session.sql(f"SELECT GET_DDL('{config['label']}', '{obj}{'()' if config['label'] == 'PROCEDURE' else ''}')").to_pandas().iloc[:, 0].values.flatten()[0]
        return { "name" : obj, "ddl" : ddl }

    def __get_stream_ddl(self, obj: dict, config: dict):
        print(f'        Copying: {config["label"]} {obj}')
        with self.session.query_history() as query_history:
            self.session.sql(f"DESC {config['label']} {obj}").collect()
            query_id = "'" + query_history.queries[0][0] + "'"
            desc_df = self.session.sql(f'SELECT "name" as stream_name, "table_name" FROM table(result_scan({query_id}))').to_pandas()   
            
            # Construct DDL         
            ddl_statement = f'CREATE OR REPLACE STREAM {desc_df.iloc[:, 0].values[0]} ON TABLE {desc_df.iloc[:, 1].values[0]};'
            return { "name" : obj, "ddl" : ddl_statement }
                

    def __get_task_ddl(self, obj: list, config: dict):
        print(f'        Copying: {config["label"]} {obj}')
        with self.session.query_history() as query_history:
            self.session.sql(f"DESC {config['label']} {obj}").collect()
            query_id = "'" + query_history.queries[0][0] + "'"
            desc_df = self.session.sql(f'SELECT "name", "warehouse", "schedule", "condition", "definition" FROM table(result_scan({query_id}))').to_pandas()  
            
            # Construct DDL
            ddl_statement = f"CREATE OR REPLACE TASK {desc_df.iloc[:, 0].values[0]} \n \
                                WAREHOUSE = {desc_df.iloc[:, 1].values[0]} \n \
                                SCHEDULE = '{desc_df.iloc[:, 2].values[0]}' \n \
                                WHEN {desc_df.iloc[:, 3].values[0]} \n \
                                AS {desc_df.iloc[:, 4].values[0]};" 
                                
        
            return { "name" : obj, "ddl" : ddl_statement }
                
                
    def __get_stage_ddl(self, obj: list, config: dict):
        print(f'        Copying: {config["label"]} {obj}')
        
        # Construct DDL
        ddl_statement = f"CREATE OR REPLACE STAGE {obj};"
        return { "name" : obj, "ddl" : ddl_statement }

        
    def __write_ddl_to_file(self, ddl_list: dict, config: dict):
        for ddl_entry in ddl_list:
            with open(f'infrastructure/{config["plural"].lower()}/{ddl_entry["name"]}.sql', 'w') as f:
                f.write(ddl_entry["ddl"])
                
    def deep_copy(self):
        print('\n\nExtracting Primary Objects')
        # For each Snowflake object type (table, view, etc.):
        for config in self.configs_list:
            print(f'\n    Working: {config["plural"]}')
            obj_list = self.list_function_mappings[config['label'].lower()](config=config)
            
            ddl_list = []
            # For each object (within Snowflake) of that type:
            for obj in obj_list:
                ddl_list.append(self.ddl_function_mappings[config['label'].lower()](obj=obj, config=config))  
                
            self.__write_ddl_to_file(ddl_list=ddl_list, config=config)