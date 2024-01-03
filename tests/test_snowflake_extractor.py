import os
import unittest
from dotenv import load_dotenv
load_dotenv()
from snowflake.snowpark import Session
from modules.snowflake_extractor import SnowflakeExtractor

class TestSnowflakeExtractor(unittest.TestCase):
    
    def setUp(self):
        # Set up a Snowflake session for testing
        connection_parameters = {
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "user": os.getenv("SNOWFLAKE_USERNAME"),
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
            "role": os.getenv("SNOWFLAKE_ROLE"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA")
        }
        try:
            self.session = Session.builder.configs(connection_parameters).create()
        except Exception as e:  
            print(f'Error: {e}')
            exit()
        
    def tearDown(self):
        self.session.close()
    
    def test_extractor(self):
        # Create a SnowflakeExtractor object with the session
        extractor = SnowflakeExtractor(session=self.session)
        
        self.assertEqual(extractor.session, self.session)
        
    
if __name__ == '__main__':
    unittest.main()