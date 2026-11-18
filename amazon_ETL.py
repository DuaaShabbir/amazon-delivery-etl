import logging
import pandas as pd
import sys
import time
import chardet
from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from snowflake.snowpark.exceptions import SnowparkSQLException



LOGGER_NAME = 'amazon_etl'

class Amazon_ETL:
    def __init__(self):
        self.logger = logging.getLogger(LOGGER_NAME)
        self.input_file_path =("amazon_delivery.csv")
        self.output_file_path = ("reencoded_amazon_delivery.csv")
        self.db_user='Your_snowflake_username'
        self.db_password='Your_snowflake_password'
        self.db_account='Your_snowflake_account'
        self.db_warehouse='Your_snowflake_warehouse'
        self.db_database_name='Your_snowflake_databasename'
        self.db_schema_name='Your_snowflake_schemaname'
        self.db_role_name='Your_snowflake_rolename'
        self.db_table_name='Your_snowflake_tablename'

    def detect_encoding(self):
        if not self.input_file_path:
            self.logger.warning("Input file path is basically empty. Returning from detect_encoding\n")
            return None
        try: 
            with open( self.input_file_path, 'rb') as f:
                result = chardet.detect(f.read(10000))
                self.logger.info(f"Encoding of the csv file in detect_encoding is: {result['encoding']}\n")
                return result['encoding']
        except Exception as e:
            self.logger.critical(f"Exception occurs in detect_encoding(): {e}\n")


    def write_csv_data(self, encd):
        if not self.input_file_path:
            self.logger.warning("Input file path is empty. Returning from write_csv_data()\n")
            return
        elif not self.output_file_path:
            self.logger.warning("Output file path is empty. Returning from write_csv_data()\n")
            return
        try:
            with open(self.input_file_path, 'r', encoding=encd, errors= 'replace') as infile:
                with open(self.output_file_path, 'w', encoding='UTF-8') as outfile:
                    for line in infile:
                        outfile.write(line)
        except FileNotFoundError:
            self.logger.error(f"File not found in write_csv_data(): {self.input_file_path}\n")     
        except PermissionError: 
            self.logger.error(f"Permission denied to read the file in write_csv_data(): {self.input_file_path}\n") 
        except UnicodeDecodeError:
            self.logger.error(f"Error while reading the file in write_csv_data()")       
        except Exception as e:
            self.logger.critical(f"Unexpected error in write_csv_data(): {e}\n")

    def extract_csv_data(self, encd:str):
        try:
            self.write_csv_data(encd)
            df = pd.read_csv(self.output_file_path)
            return df 
        except FileNotFoundError:
            self.logger.error(f'File not found in extract_csv_data(): {self.output_file_path}\n')
        except Exception as e:
            self.logger.critical(f"Unexpected error in extract_csv_data(): {e}\n")
            return pd.DataFrame

    def transform(self, df:pd.DataFrame) ->pd.DataFrame:
        try:

            # Transformation no.1 - Checking and droping duplicates
            df_dup = df[df.duplicated(keep=False, subset= df.columns)]
            if df_dup.empty:
                self.logger.info("No Duplicates found in transform():\n")
            else:
                self.logger.warning(f"Duplicate rows found in transform():\n{df_dup}\n")
                df = df.drop_duplicates()
                self.logger.info("Duplicate rows removed successfully in transform():\n")

            #Transformation no.2 - Checking and droping duplicate Order IDs as they should be unique   
            df_dupid = df[df.duplicated(keep=False, subset=['Order_ID'])]
            if not df_dupid.empty:
                self.logger.warning(f"Duplicate Order IDs are found in Dataframe in transform():{df_dupid}\n")
                df = df.drop_duplicates(subset=['Order_ID'])
                self.logger.info("Duplicate Order IDs are removed successfully in transform():\n")
            else: 
                self.logger.info("No Duplicates Order Ids are in Dataframe in transform():\n")
                
            # Transformation no.3 - Checking expected columns and filling none in that column
            expected_cols = ['Order_ID', 'Agent_Age', 'Agent_Rating', 'Store_Latitude','Store_Longitude', 'Drop_Latitude', 'Drop_Longitude', 'Order_Date','Order_Time', 'Pickup_Time', 'Weather', 'Traffic', 'Vehicle', 'Area','Delivery_Time', 'Category']
            missing_cols = [col for col in expected_cols if col not in df.columns]
            if not missing_cols:
                self.logger.info("No columns in data frame is missing in transform():\n")
            else:   
                self.logger.warning(f"Column missing in transform():{missing_cols} and filled with None\n")
                for col in missing_cols:
                    df[col]= None

            # Transformation no.4 - Checking unexpected columns and adding them in expected cols
            unexpected_cols=[col for col in df.columns if col not in expected_cols]
            if unexpected_cols:
                self.logger.warning(f"Unexpected columns in dataframe in transform():{unexpected_cols}\n")
                expected_cols.extend(unexpected_cols)
                self.logger.info(f"Added expected columns in transform(): {expected_cols}\n")
                df = df[[col for col in expected_cols if col in df.columns]]
            else:
                self.logger.info("No unexpected columns in dataframe in transform():\n")

            # Transformation no.5 - Checking null rows and dropping them
            df_all_null=df[df.isnull().all(axis=1)]
            if not df_all_null.empty:
                self.logger.warning(f"Records with all columns null found in transform()\n\n{df_all_null}\n\n")
                df=df.dropna(how='all')
                self.logger.info(f"Records with all columns null dropped in transform()\n\n:{df_all_null}\n\n")
            else:
                self.logger.info("No Records found with all columns null in transform():\n")
            
            #Checking the datatypes of columns   
            self.logger.info(f"Current Datatypes of all columns in transform():\n\n{df.dtypes}\n\n")

            # Transformation no.6 - setting the format of Date and Time Columns
            date_cols = [col for col in df.columns if "date" in col.lower()]
            for col in date_cols: 
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')
            time_cols = ['Order_Time', 'Pickup_Time']
            for col in time_cols:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%H:%M:%S')
            
            # Transformation no.7 - Merging OrderDate and OrderTime Columns  
            df['Order_DateTime'] = pd.to_datetime(df['Order_Date']) + pd.to_timedelta(df['Order_Time'], errors='coerce')
            df['Order_DateTime'] = df['Order_DateTime'].dt.strftime('%Y-%m-%d %H:%M:%S')
            df = df.drop(columns=['Order_Date', 'Order_Time'])
            
                
            #Adressing with nan
            df = df.map(lambda x:None if pd.isna(x) else x)
            
            
            # Transformation no.8 - Replacing 'NaN' string to real missing values
            text_cols = df.select_dtypes(exclude='number').columns
            for col in text_cols:
                df[col] = df[col].str.strip()
                df[col] = df[col].map(lambda x: None if x == 'NaN' else x)
            
            
            return df
        except Exception as e:
            self.logger.critical(f"Unexpected error in transformation():{e}\n")

    def table_exists(self, session):
        query=f"""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA =\'{self.db_schema_name.upper()}\'
            AND TABLE_NAME =\'{self.db_table_name.upper()}\'
            """
        result=session.sql(query).collect()
        return result[0][0] > 0
         

    def load_to_snowflake(self, df:pd.DataFrame):
        try:
            col_list = df.columns.to_list()
            self.logger.info(f"DataFrame Columns before loading into DWH in load_to_snow_flake():\n{col_list}\n")
            self.logger.info("Before creating snowflake session in load_to_snowflake()\n")
            session=Session.builder.configs({
                "user": self.db_user,
                "password":self.db_password,
                "account":self.db_account,
                "warehouse":self.db_warehouse,
                "database":self.db_database_name,
                "schema":self.db_schema_name,
                "role":self.db_role_name
            }).create()
            self.logger.info("Snowflake session successfully created in load_to_snowflake()\n")
            if self.table_exists(session):
                self.logger.info(f"Table \"{self.db_table_name}\" already exists in database in load_to_snowflake()\n")


                query = f"DESCRIBE TABLE \"{self.db_table_name}\""
                table_schema = session.sql(query).collect()
                table_cols=[row['name'].lower() for row in table_schema]
                self.logger.info(f"Table column in database in load_to_snowflake():\n{table_cols}\n")

                missing_cols=[col for col in df.columns if col.lower() not in table_cols]
                if missing_cols:
                    self.logger.warning(f"Snowflake db table has missing columns in load_to_snowflake():\n{missing_cols}\n")
                    for col in missing_cols:
                        alter_table_query=f"ALTER TABLE \"{self.db_table_name}\" ADD COLUMN \"{col}\" STRING"
                        session.sql(alter_table_query).collect()
                        self.logger.info(f"Added columns in snowflake db in load_to_snowflake():\n{df.columns.tolist()}\n")
                        
            start_time=time.time()
            session.write_pandas(
                df,
                table_name=self.db_table_name,
                overwrite=True,
                auto_create_table=True
            )
            elapsed_time=time.time()-start_time
            self.logger.info(f"Table created and data loaded into snowflake in {self.db_table_name} in {elapsed_time :.2f} seconds in load_to_snowflake()\n")
        except SnowparkSQLException as e:
            self.logger.critical(f"Snowflake sql error in load_to_snowflake(): {e}\n")
        except Exception as e:
            self.logger.critical(f"Exception occurred in load_to_snowflake(): {e}\n")
        finally:
            session.close()
            self.logger.info("Session closed successfully in load_to_snowflake()\n")

        
            
                
        
    def run(self):
        try:
            logging.basicConfig(level=logging.INFO, format= "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            self.logger.info("Started Amazon_etl in run()\n")
            encd =self.detect_encoding()
            df = self.extract_csv_data(encd)
            if df.empty:
                self.logger.warning("Dataframe is empty. Exiting from run()\n")
                return
            else:
                df_t = self.transform(df)
                sys.stdout.flush()
                sys.stderr.flush()
                self.load_to_snowflake(df_t)
            #print(encd)
            print(df)
            
            
            
        except Exception as e: 
            self.logger.critical(f"Exception occured in run(): {e}\n" )
             
        

if __name__ == '__main__':
    etl = Amazon_ETL()
    etl.run()





   
