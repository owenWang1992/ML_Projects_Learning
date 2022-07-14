from airflow import DAG
from airflow import settings
from airflow.models import Connection
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import boto3
import json


#https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/_api/airflow/providers/amazon/aws/operators/redshift_data/index.html#module-airflow.providers.amazon.aws.operators.redshift_data


REDSHIFT_CONNECTION_ID = "Redshift_BIDEV_INT"
REDSHIFT_DROP_STG_TABLE_QUERY = """
DROP TABLE IF EXISTS stage.jyoo_airflow_test;
"""
REDSHIFT_POPULATE_STG_TABLE_QUERY = """
CREATE TABLE stage.jyoo_airflow_test
    DISTSTYLE KEY 
    DISTKEY(customerid) 
    SORTKEY(customerid) 
AS
(
    SELECT  customerid,
            accountstatus,
            state,
            CONVERT_TIMEZONE('US/Pacific', GETDATE()) AS rscreatedt
    FROM    dwh.customerprofile
    ORDER
    BY      RANDOM()
    LIMIT   100
);
"""
UNLOAD_S3_BUCKET = "ecsdataservicesdev" 
UNLOAD_S3_FOLDER = "jyoo/aatest/"
UNLOAD_S3_OBJECT_NAME = "test.csv"
REDSHIFT_UNLOAD_QUERY = """
UNLOAD('select * from stage.jyoo_airflow_test;')
TO 's3://{0}/{1}'
CREDENTIALS 'aws_iam_role=arn:aws:iam::829967427875:role/redshift-copy' 
CSV DELIMITER AS '|'
HEADER
PARALLEL OFF
ALLOWOVERWRITE;
""".format(UNLOAD_S3_BUCKET,UNLOAD_S3_FOLDER+UNLOAD_S3_OBJECT_NAME)
TARGET_S3_BUCKET = "ecsdataservicesdev"
TARGET_S3_FOLDER = "jyoo2/aatest/"


def _retrieve_secret():
    client = boto3.client('secretsmanager',
        aws_access_key_id="ASIA4CPPJ5ER3MRXDJHZ",
        aws_secret_access_key="oWCEVW5AocXHWLRpEbm92gkm+vuRQgWrXnZw9qit",
        aws_session_token="FwoGZXIvYXdzEFYaDLbEinQPjk/uxZ3SGCL9AbxTi9Jjr+j7UKayi761OT2d2zsUMqMuEushu9hHEQ4ii7WUeLpvEizMkTeDyJxW8nSXOS8vTuJjr6n7r1aKizajplsIb29OdV/ZW0tb0YQCWqocjSBgAk8AJwVvIiD6jNBdwVOTKFYieYkE7XRvkkH1k8hglLSreCC+xiOX2SOYWb7o0oWd2gG5QUJn/HN324YO7kFSUwa8kdjKDisl1rxkaE2nxLrblbTD/VAl/olga8faX/1wrBT9R6D3hJ7QUo+DEGYe8OIv5/kkUIo7UyunVzypV9b/5IT18ETOpDavmqmWir9dguUUVSybHv/sx7048nx0Cr8WGDO9vqYo1OK8lgYyK8tawywAWwrVnUCPEk3aU7F8yncaAJ9y2CU7duTOOTQe6GFq0WbX3Mykccc=",
        region_name='us-west-2'
    )

    response = client.get_secret_value(
        SecretId=REDSHIFT_CONNECTION_ID
    )

    return response


def _set_redshift_connection():
    secret = _retrieve_secret()
    secretStringToDict = json.loads(secret['SecretString'])

    username = secretStringToDict['username']
    password = secretStringToDict['password']
    host = secretStringToDict['host']
    port = secretStringToDict['port']
    database = secretStringToDict['database']

    conn = Connection(
        conn_id=REDSHIFT_CONNECTION_ID,
        conn_type="redshift",
        host=host,
        login=username,
        password=password,
        port=port,
        schema=database
    )
    session = settings.Session() 
    conn_name = session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()

    if str(conn_name) == str(conn.conn_id):
        print("Connection already exists!")
    else:
        session.add(conn)
        session.commit() 


def _copy_s3_object(sourceBucket,sourceFolder,targetBucket,targetFolder):
    s3 = boto3.client('s3',
        aws_access_key_id="ASIA4CPPJ5ER3MRXDJHZ",
        aws_secret_access_key="oWCEVW5AocXHWLRpEbm92gkm+vuRQgWrXnZw9qit",
        aws_session_token="FwoGZXIvYXdzEFYaDLbEinQPjk/uxZ3SGCL9AbxTi9Jjr+j7UKayi761OT2d2zsUMqMuEushu9hHEQ4ii7WUeLpvEizMkTeDyJxW8nSXOS8vTuJjr6n7r1aKizajplsIb29OdV/ZW0tb0YQCWqocjSBgAk8AJwVvIiD6jNBdwVOTKFYieYkE7XRvkkH1k8hglLSreCC+xiOX2SOYWb7o0oWd2gG5QUJn/HN324YO7kFSUwa8kdjKDisl1rxkaE2nxLrblbTD/VAl/olga8faX/1wrBT9R6D3hJ7QUo+DEGYe8OIv5/kkUIo7UyunVzypV9b/5IT18ETOpDavmqmWir9dguUUVSybHv/sx7048nx0Cr8WGDO9vqYo1OK8lgYyK8tawywAWwrVnUCPEk3aU7F8yncaAJ9y2CU7duTOOTQe6GFq0WbX3Mykccc="
    )

    response = s3.list_objects_v2(
        Bucket=sourceBucket,
        Prefix=sourceFolder
    )
    keys = []
    for obj in response['Contents']:
        keys.append(obj['Key'])
    
    for k in keys:
        filename = k.split("/")[-1]
        response = s3.copy_object(
            CopySource={'Bucket':sourceBucket, 'Key': k},
            Bucket=targetBucket,
            Key=targetFolder+filename
        )


def _add_emr_step():
    emr = boto3.client('emr',
        aws_access_key_id="ASIA4CPPJ5ER3MRXDJHZ",
        aws_secret_access_key="oWCEVW5AocXHWLRpEbm92gkm+vuRQgWrXnZw9qit",
        aws_session_token="FwoGZXIvYXdzEFYaDLbEinQPjk/uxZ3SGCL9AbxTi9Jjr+j7UKayi761OT2d2zsUMqMuEushu9hHEQ4ii7WUeLpvEizMkTeDyJxW8nSXOS8vTuJjr6n7r1aKizajplsIb29OdV/ZW0tb0YQCWqocjSBgAk8AJwVvIiD6jNBdwVOTKFYieYkE7XRvkkH1k8hglLSreCC+xiOX2SOYWb7o0oWd2gG5QUJn/HN324YO7kFSUwa8kdjKDisl1rxkaE2nxLrblbTD/VAl/olga8faX/1wrBT9R6D3hJ7QUo+DEGYe8OIv5/kkUIo7UyunVzypV9b/5IT18ETOpDavmqmWir9dguUUVSybHv/sx7048nx0Cr8WGDO9vqYo1OK8lgYyK8tawywAWwrVnUCPEk3aU7F8yncaAJ9y2CU7duTOOTQe6GFq0WbX3Mykccc=",
        region_name='us-west-2'
    )

    response = emr.add_job_flow_steps(
        JobFlowId="j-2I0QT1W5S6LMO",
        Steps=[
            {
                'Name': 'ets_test',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'sh', '/mnt1/dev/DataLoader/Shell/engagementtargeting_modeloutput_incremental_autoinsurance.sh'
                    ]
                }
            },
        ]
    )


with DAG(
    dag_id='redshift_test', 
    start_date=datetime(2022,1,1), 
    schedule_interval='@daily', 
    catchup=False
) as dag:
   
    drop_stg_table = RedshiftSQLOperator(
         task_id='drop_stg_table',
         redshift_conn_id=REDSHIFT_CONNECTION_ID,
         sql=REDSHIFT_DROP_STG_TABLE_QUERY,
         autocommit=True
    )

    create_populate_stg_table = RedshiftSQLOperator(
        task_id='create_populate_stg_table',
        redshift_conn_id=REDSHIFT_CONNECTION_ID,
        sql=REDSHIFT_POPULATE_STG_TABLE_QUERY,
        autocommit=True
    )

    unload_stg_table = RedshiftSQLOperator(
        task_id='unload_stg_table',
        redshift_conn_id=REDSHIFT_CONNECTION_ID,
        sql=REDSHIFT_UNLOAD_QUERY,
        autocommit=True
    )

    copy_s3_object = PythonOperator(
        task_id='copy_s3_object',
        python_callable=_copy_s3_object,
        op_kwargs={
            "sourceBucket":UNLOAD_S3_BUCKET,
            "sourceFolder":UNLOAD_S3_FOLDER,
            "targetBucket":TARGET_S3_BUCKET,
            "targetFolder":TARGET_S3_FOLDER
        }
    )

    get_redshift_connection = PythonOperator(
        task_id='get_redshift_connection',
        python_callable=_set_redshift_connection
    )

    add_emr_step = PythonOperator(
        task_id='add_emr_step',
        python_callable=_add_emr_step
    )

    get_redshift_connection >> drop_stg_table >> create_populate_stg_table >> unload_stg_table >> copy_s3_object >> add_emr_step
