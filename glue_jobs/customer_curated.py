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

# Script generated for node accelerator_trusted
accelerator_trusted_node1718451399189 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="accelerometer_trusted", transformation_ctx="accelerator_trusted_node1718451399189")

# Script generated for node customer_trusted
customer_trusted_node1718451398395 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="customer_trusted", transformation_ctx="customer_trusted_node1718451398395")

# Script generated for node SQL Query
SqlQuery0 = '''
select distinct ct.*
from customer_trusted ct
join accelerometer_trusted at 
on ct.email = at.user
'''
SQLQuery_node1718449856009 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_landing":customer_trusted_node1718451398395, "accelerator_landing":accelerator_trusted_node1718451399189}, transformation_ctx = "SQLQuery_node1718449856009")

# Script generated for node customer_curated
customer_curated_node1718449857978 = glueContext.getSink(path="s3://pyspark-glue-udacity/customer_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="customer_curated_node1718449857978")
customer_curated_node1718449857978.setCatalogInfo(catalogDatabase="stedi_project",catalogTableName="customer_curated")
customer_curated_node1718449857978.setFormat("json")
customer_curated_node1718449857978.writeFrame(SQLQuery_node1718449856009)
job.commit()