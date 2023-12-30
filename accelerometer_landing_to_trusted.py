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
AccelerometerLanding_node1703884416088 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-lf/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1703884416088",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1703884707367 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-lf/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1703884707367",
)

# Script generated for node Customer Privacy
CustomerPrivacy_node1703885194913 = Join.apply(
    frame1=AccelerometerLanding_node1703884416088,
    frame2=CustomerTrusted_node1703884707367,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="CustomerPrivacy_node1703885194913",
)

# Script generated for node Drop Fields
DropFields_node1703885296355 = DropFields.apply(
    frame=CustomerPrivacy_node1703885194913,
    paths=[
        "serialNumber",
        "birthDay",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "shareWithFriendsAsOfDate",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithPublicAsOfDate",
    ],
    transformation_ctx="DropFields_node1703885296355",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1703885329172 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1703885296355,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house-lf/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AccelerometerTrusted_node1703885329172",
)

job.commit()
