from snowflake.snowpark import Session


class DataDeepCopy:
    def __init__(self, session: Session):
        self.session = session    
        
    def __get_all_stages(self):
        with self.session.query_history() as query_history:
            self.session.sql(f"SHOW STAGES;").collect()
            query_id = "'" + query_history.queries[0][0] + "'"
            obj_list = self.session.sql(f'SELECT "name" FROM table(result_scan({query_id}))').to_pandas().iloc[:, 0].values.flatten().tolist()
            return obj_list
        
    def __get_all_tables(self):
        obj_df = self.session.sql(f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'PUBLIC' AND TABLE_TYPE = 'BASE TABLE'; ").to_pandas()
        obj_list = obj_df.iloc[:, 0].values.flatten().tolist()
        return obj_list


    def __get_table_data(self, table_name: str):  
        self.session.sql(f"SELECT * FROM {table_name};").to_pandas().to_csv(f"snowflake/data/table_data/{table_name}.csv")
        
            
    def deep_copy(self):
        print('\n\nExtracting Stage Data')
        for stage in self.__get_all_stages():
            print(f'    Copying Data - Stage: {stage}')
            try:
                self.session.file.get(f"@{stage.lower()}", f"snowflake/data/stage_data/{stage}")
            except Exception as e:
                print(f'        ERROR: {e}')
                print(f'    Skipping Stage: {stage}')
                continue
           
        print('\n\nExtracting Table Data') 
        for table in self.__get_all_tables():
            print(f'    Copying Data - Table: {table}')
            self.__get_table_data(table_name=table)