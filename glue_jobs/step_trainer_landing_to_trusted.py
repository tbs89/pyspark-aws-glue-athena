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

# Script generated for node customer_curated
customer_curated_node1718453037757 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="customer_curated", transformation_ctx="customer_curated_node1718453037757")

# Script generated for node step_trainer_landing
step_trainer_landing_node1718453039567 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="step_trainer_landing", transformation_ctx="step_trainer_landing_node1718453039567")

# Script generated for node SQL Query
SqlQuery0 = '''
select distinct sl.* 
from step_trainer_landing sl  
join customer_curated cc 
on sl.serialnumber = cc.serialnumber;

'''
SQLQuery_node1718453041455 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step_trainer_landing":step_trainer_landing_node1718453039567, "customer_curated":customer_curated_node1718453037757}, transformation_ctx = "SQLQuery_node1718453041455")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1718453043605 = glueContext.getSink(path="s3://pyspark-glue-udacity/step_trainer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="step_trainer_trusted_node1718453043605")
step_trainer_trusted_node1718453043605.setCatalogInfo(catalogDatabase="stedi_project",catalogTableName="step_trainer_trusted")
step_trainer_trusted_node1718453043605.setFormat("json")
step_trainer_trusted_node1718453043605.writeFrame(SQLQuery_node1718453041455)
job.commit()


