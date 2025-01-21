import pandas as pd

# Snowflake connector libraries
import snowflake.connector as snow
from snowflake.connector.pandas_tools import write_pandas
import boto3

ssm = boto3.client('ssm',region_name='us-east-1')
s3 = boto3.client('s3',region_name='us-east-1')

## Please ensure these parameters are added in the AWS Systems Manager and the EC2 instance IAM has access to s3 and the ssm services
sf_username = ssm.get_parameter(Name='/snowflake/username', WithDecryption=True)['Parameter']['Value']
sf_password = ssm.get_parameter(Name='/snowflake/password', WithDecryption=True)['Parameter']['Value']
sf_account = ssm.get_parameter(Name='/snowflake/accountname', WithDecryption=True)['Parameter']['Value']



def run_script():

   #Module to create the snowflake connection and return the connection objects
   def create_connection():
      conn = snow.connect(user=sf_username,
      password=sf_password,
      account=sf_account,
      warehouse="COMPUTE_WH",
      database="PROD",
      schema="DBT_RAW")
      cursor = conn.cursor()
      print('SQL Connection Created')
      return cursor,conn

   # Module to truncate the table if exists. This will ensure duplicate load doesn't happen
   def truncate_table():
      cur,conn=create_connection()
      sql_data2 = "TRUNCATE TABLE IF EXISTS DATA2_RAW"
      sql_data1 = "TRUNCATE TABLE IF EXISTS DATA1_RAW"
      cur.execute(sql_data2)
      cur.execute(sql_data1)
      print('Tables truncated')

   #Module to read csv file and load data in Snowflake. Table is created dynamically
   def load_data():
      titles_file = s3.get_object(Bucket='propdata', Key='raw_files/data2.csv')
      credits_file = s3.get_object(Bucket='propdata', Key='raw_files/data1.csv')
      
      cur,conn=create_connection()
      delimiter = "," # Replace if you're using a different delimiter.

      t_df = pd.read_csv(titles_file['Body'], sep = delimiter)
      print("Data2 file read")
      c_df = pd.read_csv(credits_file['Body'], sep = delimiter)
      print("Data1 file read")

      write_pandas(conn, t_df, "data2",auto_create_table=True)
      print("Data2 file written")
      write_pandas(conn, c_df, "data1",auto_create_table=True)
      print("Data1 file written")

      cur = conn.cursor()


      # Close your cursor and your connection.
      cur.close()
      conn.close()

   print("Starting Script")
   truncate_table()
   load_data()
