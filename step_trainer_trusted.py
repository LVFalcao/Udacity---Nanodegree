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

# Script generated for node Customer Curated
CustomerCurated_node1703888144072 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-lf/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCurated_node1703888144072",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1703888193540 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-lf/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1703888193540",
)

# Script generated for node Join
Join_node1703888260233 = Join.apply(
    frame1=CustomerCurated_node1703888144072,
    frame2=StepTrainerLanding_node1703888193540,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1703888260233",
)

# Script generated for node Drop Fields
DropFields_node1703888289073 = DropFields.apply(
    frame=Join_node1703888260233,
    paths=[
        "`.serialNumber`",
        "registrationDate",
        "lastUpdateDate",
        "shareWithFriendsAsOfDate",
        "shareWithResearchAsOfDate",
        "shareWithPublicAsOfDate",
        "customerName",
        "email",
        "phone",
        "birthDay",
    ],
    transformation_ctx="DropFields_node1703888289073",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1703888493242 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1703888289073,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house-lf/step_trainer/trusted/",
        "compression": "snappy",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrusted_node1703888493242",
)

job.commit()
