import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Landing
CustomerLanding_node1703882405489 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-lf/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1703882405489",
)

# Script generated for node Privacy Filter
PrivacyFilter_node1704023486134 = Filter.apply(
    frame=CustomerLanding_node1703882405489,
    f=lambda row: (not (row["sharewithresearchasofdate"] == 0)),
    transformation_ctx="PrivacyFilter_node1704023486134",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1703882702428 = glueContext.getSink(
    path="s3://stedi-lake-house-lf/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerTrusted_node1703882702428",
)
CustomerTrusted_node1703882702428.setCatalogInfo(
    catalogDatabase="stedi3", catalogTableName="customer_trusted"
)
CustomerTrusted_node1703882702428.setFormat("json")
CustomerTrusted_node1703882702428.writeFrame(PrivacyFilter_node1704023486134)
job.commit()
