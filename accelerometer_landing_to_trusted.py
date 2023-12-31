import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1704023875594 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-lf/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1704023875594",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1704023902279 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-lf/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1704023902279",
)

# Script generated for node Join
Join_node1704023931571 = Join.apply(
    frame1=CustomerTrusted_node1704023902279,
    frame2=AccelerometerLanding_node1704023875594,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1704023931571",
)

# Script generated for node Drop Fields
DropFields_node1704023950242 = DropFields.apply(
    frame=Join_node1704023931571,
    paths=[
        "sharewithfriendsasofdate",
        "sharewithpublicasofdate",
        "email",
        "customername",
        "registrationdate",
        "lastupdatedate",
        "phone",
        "serialnumber",
        "sharewithresearchasofdate",
        "birthday",
    ],
    transformation_ctx="DropFields_node1704023950242",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1704024003073 = glueContext.getSink(
    path="s3://stedi-lake-house-lf/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrusted_node1704024003073",
)
AccelerometerTrusted_node1704024003073.setCatalogInfo(
    catalogDatabase="stedi3", catalogTableName="accelerometer_trusted"
)
AccelerometerTrusted_node1704024003073.setFormat("json")
AccelerometerTrusted_node1704024003073.writeFrame(DropFields_node1704023950242)
job.commit()
