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

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1718453338904 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="step_trainer_trusted", transformation_ctx="step_trainer_trusted_node1718453338904")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1718453341254 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1718453341254")

# Script generated for node SQL Query
SqlQuery0 = '''
select st.serialnumber, st.sensorreadingtime, st.distancefromobject,
       at.user, at.timestamp, at.x, at.y, at.z    
from step_trainer_trusted st 
join accelerometer_trusted at 
on st.sensorreadingtime = at.timestamp;
'''
SQLQuery_node1718453427249 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"accelerometer_trusted":accelerometer_trusted_node1718453341254, "step_trainer_trusted":step_trainer_trusted_node1718453338904}, transformation_ctx = "SQLQuery_node1718453427249")

# Script generated for node machine_learning_curated
machine_learning_curated_node1718453478105 = glueContext.getSink(path="s3://pyspark-glue-udacity/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="machine_learning_curated_node1718453478105")
machine_learning_curated_node1718453478105.setCatalogInfo(catalogDatabase="stedi_project",catalogTableName="machine_learning_curated")
machine_learning_curated_node1718453478105.setFormat("json")
machine_learning_curated_node1718453478105.writeFrame(SQLQuery_node1718453427249)
job.commit()