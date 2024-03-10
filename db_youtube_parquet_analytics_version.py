"""
Description:

Dynamic Frame:
In AWS Glue, a dynamic frame is a distributed data structure similar to a DataFrame in Apache Spark or a table in a relational database.
It is a high-level abstraction that allows for the representation and manipulation of semi-structured or unstructured data, such as JSON, CSV, or Parquet files.
Dynamic frames provide a schema-on-read approach, meaning that they infer the schema of the data at runtime rather than requiring a predefined schema.
They offer flexibility in handling data with varying or evolving schemas, making them particularly suitable for processing big data in ETL (Extract, Transform, Load) workflows.
Dynamic frames support various operations like filtering, joining, transforming, and writing data to different data stores, enabling efficient data integration and transformation pipelines.

1. Initialization:
Import necessary modules and initialize the Glue job with provided arguments.

2. AWS Glue Data Catalog:
Create dynamic frames from tables stored in the AWS Glue Data Catalog. These tables contain cleaned and transformed data.

3. Joining:
Perform an inner join operation between the two dynamic frames based on the "category_id" column from the first frame and the "id" column from the second frame.

4. Amazon S3 Sink:
Configure the sink to write the joined dynamic frame to an Amazon S3 bucket.
Specify the path where the data will be written and set partition keys for efficient data organization.
Enable catalog updates to reflect changes in the Glue Data Catalog.
Set the output format to Glue Parquet with Snappy compression for optimized storage.

5. Committing Job:
Commit the Glue job, indicating successful execution.


"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Retrieve job name from command-line arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize SparkContext
sc = SparkContext()

# Initialize GlueContext
glueContext = GlueContext(sc)

# Get SparkSession from GlueContext
spark = glueContext.spark_session



# Commit the job
job.commit()
