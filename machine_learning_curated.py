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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1703889013479 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-lf/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerTrusted_node1703889013479",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1703888980567 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-lf/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1703888980567",
)

# Script generated for node Join
Join_node1703889314654 = Join.apply(
    frame1=AccelerometerTrusted_node1703888980567,
    frame2=StepTrainerTrusted_node1703889013479,
    keys1=["timestamp"],
    keys2=["sensorreadingtime"],
    transformation_ctx="Join_node1703889314654",
)

# Script generated for node Drop Fields
DropFields_node1703889332433 = DropFields.apply(
    frame=Join_node1703889314654,
    paths=["user"],
    transformation_ctx="DropFields_node1703889332433",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1703889347617 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1703889332433,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house-lf/machine-learning-curated/",
        "partitionKeys": [],
    },
    transformation_ctx="MachineLearningCurated_node1703889347617",
)

job.commit()
