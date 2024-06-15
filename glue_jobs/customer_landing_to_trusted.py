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

# Script generated for node customer_landing
customer_landing_node1718448404713 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="customer_landing", transformation_ctx="customer_landing_node1718448404713")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT * FROM customer_landing
WHERE shareWithResearchAsOfDate IS NOT NULL
'''
SQLQuery_node1718448415777 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_landing":customer_landing_node1718448404713}, transformation_ctx = "SQLQuery_node1718448415777")

# Script generated for node customer_trusted
customer_trusted_node1718448417685 = glueContext.getSink(path="s3://pyspark-glue-udacity/customer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="customer_trusted_node1718448417685")
customer_trusted_node1718448417685.setCatalogInfo(catalogDatabase="stedi_project",catalogTableName="customer_trusted")
customer_trusted_node1718448417685.setFormat("json")
customer_trusted_node1718448417685.writeFrame(SQLQuery_node1718448415777)
job.commit()
