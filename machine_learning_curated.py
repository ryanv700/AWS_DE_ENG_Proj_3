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

# Script generated for node Step Trainer trusted
StepTrainertrusted_node1715615294014 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="step_trainer_trusted", transformation_ctx="StepTrainertrusted_node1715615294014")

# Script generated for node Accelerometer_trusted
Accelerometer_trusted_node1715615316018 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="accelerometer_trusted", transformation_ctx="Accelerometer_trusted_node1715615316018")

# Script generated for node SQL Query
SqlQuery2434 = '''
SELECT
  *
FROM
  step_trainer_trusted s
JOIN accelerometer_trusted a on S.sensorreadingtime = a.timestamp
'''
SQLQuery_node1715616410270 = sparkSqlQuery(glueContext, query = SqlQuery2434, mapping = {"step_trainer_trusted":StepTrainertrusted_node1715615294014, "accelerometer_trusted":Accelerometer_trusted_node1715615316018}, transformation_ctx = "SQLQuery_node1715616410270")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1715616046377 = glueContext.getSink(path="s3://stedi-lakehouse-project/machine_leaerning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1715616046377")
StepTrainerTrusted_node1715616046377.setCatalogInfo(catalogDatabase="stedi-db",catalogTableName="machine_learning_curated")
StepTrainerTrusted_node1715616046377.setFormat("json")
StepTrainerTrusted_node1715616046377.writeFrame(SQLQuery_node1715616410270)
job.commit()