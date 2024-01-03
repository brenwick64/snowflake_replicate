from snowflake.snowpark import Session

class SnowflakeExtractor:
    def __init__(self, session: Session) -> None:
        self.session = session
        pass
    
    def extract_list(self, snowflake_object_type: str) -> list:
        pass
    
    def extract_ddl(self, snowflake_object_name: str) -> str:
        pass
    
    
