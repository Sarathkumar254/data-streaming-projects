import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame
import concurrent.futures
import re

class GroupFilter:
      def __init__(self, name, filters):
        self.name = name
        self.filters = filters

def apply_group_filter(source_DyF, group):
    return(Filter.apply(frame = source_DyF, f = group.filters))

def threadedRoute(glue_ctx, source_DyF, group_filters) -> DynamicFrameCollection:
    dynamic_frames = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_filter = {executor.submit(apply_group_filter, source_DyF, gf): gf for gf in group_filters}
        for future in concurrent.futures.as_completed(future_to_filter):
            gf = future_to_filter[future]
            if future.exception() is not None:
                print('%r generated an exception: %s' % (gf, future.exception()))
            else:
                dynamic_frames[gf.name] = future.result()
    return DynamicFrameCollection(dynamic_frames, glue_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node s3_data_source
s3_data_source_node1743456891123 = glueContext.create_dynamic_frame.from_catalog(database="movie-data", table_name="imdb_movies_rating_csv", transformation_ctx="s3_data_source_node1743456891123")

# Script generated for node data quality checks
dataqualitychecks_node1743457571571_ruleset = """
    # Example rules: Completeness "colA" between 0.4 and 0.8, ColumnCount > 10
    Rules = [
        IsComplete "imdb_rating",
        ColumnValues "imdb_rating" between 6.6 and 10.3
    ]
"""

dataqualitychecks_node1743457571571 = EvaluateDataQuality().process_rows(frame=s3_data_source_node1743456891123, ruleset=dataqualitychecks_node1743457571571_ruleset, publishing_options={"dataQualityEvaluationContext": "dataqualitychecks_node1743457571571", "enableDataQualityCloudWatchMetrics": True, "enableDataQualityResultsPublishing": True}, additional_options={"observations.scope":"ALL","performanceTuning.caching":"CACHE_NOTHING"})

# Script generated for node rowLevelOutcomes
rowLevelOutcomes_node1743458041939 = SelectFromCollection.apply(dfc=dataqualitychecks_node1743457571571, key="rowLevelOutcomes", transformation_ctx="rowLevelOutcomes_node1743458041939")

# Script generated for node ruleOutcomes
ruleOutcomes_node1743457925858 = SelectFromCollection.apply(dfc=dataqualitychecks_node1743457571571, key="ruleOutcomes", transformation_ctx="ruleOutcomes_node1743457925858")

# Script generated for node Conditional Router
ConditionalRouter_node1743458581730 = threadedRoute(glueContext,
  source_DyF = rowLevelOutcomes_node1743458041939,
  group_filters = [GroupFilter(name = "output_group_1", filters = lambda row: (bool(re.match("Failed", row["DataQualityEvaluationResult"])))), GroupFilter(name = "default_group", filters = lambda row: (not(bool(re.match("Failed", row["DataQualityEvaluationResult"])))))])

# Script generated for node default_group
default_group_node1743458582574 = SelectFromCollection.apply(dfc=ConditionalRouter_node1743458581730, key="default_group", transformation_ctx="default_group_node1743458582574")

# Script generated for node output_group_1
output_group_1_node1743458582798 = SelectFromCollection.apply(dfc=ConditionalRouter_node1743458581730, key="output_group_1", transformation_ctx="output_group_1_node1743458582798")

# Script generated for node drop columns
dropcolumns_node1743458914855 = ApplyMapping.apply(frame=default_group_node1743458582574, mappings=[("DataQualityEvaluationResult", "string", "DataQualityEvaluationResult", "string")], transformation_ctx="dropcolumns_node1743458914855")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=ruleOutcomes_node1743457925858, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1743456794217", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1743458297477 = glueContext.write_dynamic_frame.from_options(frame=ruleOutcomes_node1743457925858, connection_type="s3", format="json", connection_options={"path": "s3://movies-data-analysis-123/rule_outcome/", "compression": "uncompressed", "partitionKeys": []}, transformation_ctx="AmazonS3_node1743458297477")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=output_group_1_node1743458582798, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1743456794217", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1743458777943 = glueContext.write_dynamic_frame.from_options(frame=output_group_1_node1743458582798, connection_type="s3", format="json", connection_options={"path": "s3://movies-data-analysis-123/bad_records/", "compression": "uncompressed", "partitionKeys": []}, transformation_ctx="AmazonS3_node1743458777943")

# Script generated for node readshift-load
readshiftload_node1743459229741 = glueContext.write_dynamic_frame.from_options(frame=dropcolumns_node1743458914855, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-151748497025-eu-north-1/temporary/", "useConnectionProperties": "true", "dbtable": "movies.imdb_movies_rating", "connectionName": "redshift_conn", "preactions": "CREATE TABLE IF NOT EXISTS movies.imdb_movies_rating (DataQualityEvaluationResult VARCHAR);"}, transformation_ctx="readshiftload_node1743459229741")

job.commit()