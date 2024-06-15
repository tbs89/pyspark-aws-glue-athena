import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node customer_trusted
customer_trusted_node1718448841847 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="customer_trusted", transformation_ctx="customer_trusted_node1718448841847")

# Script generated for node accelerometer_landing
accelerometer_landing_node1718448847964 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="accelerometer_landing", transformation_ctx="accelerometer_landing_node1718448847964")

# Script generated for node SQL Query
SqlQuery0 = '''
select distinct al.user, al.timestamp, al.x, al.y, al.z 
from accelerometer_landing al 
join customer_trusted ct 
on al.user = ct.email
'''
SQLQuery_node1718448849682 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"accelerometer_landing":accelerometer_landing_node1718448847964, "customer_trusted":customer_trusted_node1718448841847}, transformation_ctx = "SQLQuery_node1718448849682")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1718448852001 = glueContext.getSink(path="s3://pyspark-glue-udacity/accelerometer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="accelerometer_trusted_node1718448852001")
accelerometer_trusted_node1718448852001.setCatalogInfo(catalogDatabase="stedi_project",catalogTableName="accelerometer_trusted")
accelerometer_trusted_node1718448852001.setFormat("json")
accelerometer_trusted_node1718448852001.writeFrame(SQLQuery_node1718448849682)
job.commit()