import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1715606969680 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lakehouse-project/customer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1715606969680")

# Script generated for node Share with Research Filter
SharewithResearchFilter_node1715606977132 = Filter.apply(frame=AmazonS3_node1715606969680, f=lambda row: (not(row["sharewithresearchasofdate"] == 0)), transformation_ctx="SharewithResearchFilter_node1715606977132")

# Script generated for node Customer Trusted
CustomerTrusted_node1715606981672 = glueContext.getSink(path="s3://stedi-lakehouse-project/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1715606981672")
CustomerTrusted_node1715606981672.setCatalogInfo(catalogDatabase="stedi-db",catalogTableName="customer_trusted")
CustomerTrusted_node1715606981672.setFormat("json")
CustomerTrusted_node1715606981672.writeFrame(SharewithResearchFilter_node1715606977132)
job.commit()