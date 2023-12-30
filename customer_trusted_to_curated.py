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
AccelerometerLanding_node1703887405475 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-lf/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1703887405475",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1703887333666 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-lf/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1703887333666",
)

# Script generated for node Privacy Filter
PrivacyFilter_node1703887438177 = Join.apply(
    frame1=AccelerometerLanding_node1703887405475,
    frame2=CustomerTrusted_node1703887333666,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="PrivacyFilter_node1703887438177",
)

# Script generated for node Drop Fields
DropFields_node1703887459618 = DropFields.apply(
    frame=PrivacyFilter_node1703887438177,
    paths=["user", "z", "y", "x", "timestamp"],
    transformation_ctx="DropFields_node1703887459618",
)

# Script generated for node Customer Curated
CustomerCurated_node1703887519400 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1703887459618,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house-lf/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerCurated_node1703887519400",
)

job.commit()
