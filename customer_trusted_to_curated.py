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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1715615294014 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1715615294014")

# Script generated for node Customer Trusted
CustomerTrusted_node1715615316018 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1715615316018")

# Script generated for node Join
Join_node1715615409686 = Join.apply(frame1=AccelerometerTrusted_node1715615294014, frame2=CustomerTrusted_node1715615316018, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1715615409686")

# Script generated for node SQL Query
SqlQuery2453 = '''
SELECT DISTINCT customername, email, phone, birthday,
    serialnumber, registrationdate, lastupdatedate, 
    sharewithresearchasofdate, sharewithpublicasofdate
FROM myDataSource;
'''
SQLQuery_node1715616410270 = sparkSqlQuery(glueContext, query = SqlQuery2453, mapping = {"myDataSource":Join_node1715615409686}, transformation_ctx = "SQLQuery_node1715616410270")

# Script generated for node Customer Curated
CustomerCurated_node1715616046377 = glueContext.getSink(path="s3://stedi-lakehouse-project/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1715616046377")
CustomerCurated_node1715616046377.setCatalogInfo(catalogDatabase="stedi-db",catalogTableName="customer_curated")
CustomerCurated_node1715616046377.setFormat("json")
CustomerCurated_node1715616046377.writeFrame(SQLQuery_node1715616410270)
job.commit()