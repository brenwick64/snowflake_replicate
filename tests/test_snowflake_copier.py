import os
import unittest
from dotenv import load_dotenv 
load_dotenv()
from snowflake.snowpark import Session
from modules.snowflake_copier import SnowflakeCopier

class TestSnowflakeCopier(unittest.TestCase):

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
            self.copier = SnowflakeCopier(session=self.session, database=os.getenv("SNOWFLAKE_DATABASE"), schema=os.getenv("SNOWFLAKE_SCHEMA"))
        except Exception as e:  
            print(f'Error: {e}')
            exit()
    
            
    def tearDown(self):
        self.session.close()    
        
        
    def test_get_object_list(self):
        result = self.copier.get_object_list('TABLE')
        self.assertIn('TRANSACTIONS', result)
        self.assertIn('BUDGET', result)
        
        
    def test_get_object_ddl(self):
        result = self.copier.get_object_ddl('TABLE', 'TRANSACTIONS')
        self.assertEqual(type(result), str)
        
if __name__ == '__main__':
    unittest.main()
