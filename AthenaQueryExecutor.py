import time
from boto import config
import boto3
import pandas as pd
import io

from rsa import verify
import Config

class AthenaQueryExecutor:

    def __init__(self, query):
        self.database = Config.database
        self.folder = 'QueryReconcilator/Demo/'
        self.bucket = 'athenaworkrepo-aws-poc'
        self.s3_output =  's3://' + self.bucket + '/' + self.folder
        self.region_name = Config.AWS_REGION
        self.aws_access_key_id = Config.AWS_ACCESS_KEY
        self.aws_secret_access_key = Config.AWS_SECRET_KEY
        self.query = query

    def load_conf(self, q):
        try:
            self.client = boto3.client('athena', 
                              region_name = self.region_name, 
                              aws_access_key_id = self.aws_access_key_id,
                              aws_secret_access_key= self.aws_secret_access_key,verify=False)
            response = self.client.start_query_execution(
                QueryString = q,
                    QueryExecutionContext={
                    'Database': self.database
                    },
                    ResultConfiguration={
                    'OutputLocation': self.s3_output,
                    }
            )
            self.filename = response['QueryExecutionId']
            

        except Exception as e:
            print(e)
        return response                

    def run_query(self):
        queries = [self.query]
        for q in queries:
            res = self.load_conf(q)
        try:              
            query_status = None
            while query_status == 'QUEUED' or query_status == 'RUNNING' or query_status is None:
                query_status = self.client.get_query_execution(QueryExecutionId=res["QueryExecutionId"])['QueryExecution']['Status']['State']
                print("Athena Query is ", query_status)
                if query_status == 'FAILED' or query_status == 'CANCELLED':
                    raise Exception('Athena query with the string "{}" failed or was cancelled'.format(self.query))
                time.sleep(10)
            

            df = self.obtain_data()
            return df

        except Exception as e:
            print(e)      

    def obtain_data(self):
        try:
            self.resource = boto3.resource('s3', 
                                  region_name = self.region_name, 
                                  aws_access_key_id = self.aws_access_key_id,
                                  aws_secret_access_key= self.aws_secret_access_key,verify=False)

            response = self.resource \
            .Bucket(self.bucket) \
            .Object(key= self.folder + self.filename + '.csv') \
            .get()

            return pd.read_csv(io.BytesIO(response['Body'].read()), encoding='utf8')   
        except Exception as e:
            print(e)  
