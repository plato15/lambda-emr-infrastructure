from __future__ import print_function
#import org.apache.spark.sql.types._
import sys
import math
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
#from pyspark.sql.types import StructType
from datetime import date

## @params: [JOB_NAME]
print()
if __name__ == "__main__":
    sc = SparkContext(appName="Curated_mdm")
    spark = SparkSession(sc)
    #glueContext = GlueContext(sc)


    # Convert reading_type to human readable format.
    # Takes a String reading_type.
    # Returns a String reading_type_desc.
    # Python dictionary is queried using Dictionary.get().
    #   The dictionary.get() function takes a String key, and a String default result.
    #   The usage here returns the original reading type as the result.
    def hrf_reading_type(reading_type):
        switcher = {
            "0.0.2.4.19.1.12.0.0.0.0.0.0.0.0.3.72.0": "KWH_REC", 
            "0.0.2.4.1.1.12.0.0.0.0.0.0.0.0.3.72.0": "KWH_DEL", 
            "0.0.2.4.1.1.12.0.0.0.0.0.0.0.0.3.73.0": "KVARH",
            "0.0.2.4.1.1.12.0.0.0.0.0.0.0.0.3.71.0": "KVAH", 
            "0.2.2.0.0.1.54.0.0.0.0.0.0.0.128.0.29.0": "VRMS_phaseA", 
            "0.0.2.12.0.1.12.0.0.0.0.0.0.0.64.3.29.0": "VRMS_phaseB", 
            "0.2.2.0.0.1.54.0.0.0.0.0.0.0.64.0.29.0": "VRMS_phaseB",
            "0.2.2.0.0.1.54.0.0.0.0.0.0.0.32.0.29.0": "VRMS_phaseC", 
            "0.2.2.0.0.1.4.0.0.0.0.0.0.0.128.0.5.0": "IRMS_PhaseA", 
            "0.2.2.0.0.1.4.0.0.0.0.0.0.0.64.0.5.0": "IRMS_PhaseB", 
            "0.2.2.0.0.1.4.0.0.0.0.0.0.0.32.0.5.0": "IRMS_PhaseC"
        }
        return switcher.get(reading_type,reading_type)

    # instantiate function as UDF.
    udf_hrf_reading_type = udf(hrf_reading_type)

    # Convert reading_quality to human readable format.
    # Takes a String reading_quality.
    # Returns a String reading_quality_desc.
    # Python dictionary is queried using Dictionary.get().
    #   The dictionary.get() function takes a String key, and a String default result.
    #   The usage here returns the original reading quality code as the result.
    def hrf_estimate_actual(reading_quality):
        switcher = {
            "3.8.0": "Estimate",
            "3.0.0": "Actual",
        }
        return switcher.get(reading_quality,"Unknown")

    # Instantiate function as UDF.
    udf_hrf_estimate_actual = udf(hrf_estimate_actual)

    ## @type: DataSource
    ## @args: [database = "datalake_prod_raw", table_name = "mdm", transformation_ctx = "datasource0"]
    ## @return: datasource0
    ## @inputs: []
    #datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "sampledb", table_name = "mdm_test", transformation_ctx = "datasource0")
    customSchema = StructType([
        StructField("record_type", StringType(), True),
        StructField("meter_id", StringType(), True),
        StructField("service_point_id", StringType(), True),
        StructField("reading_type", StringType(), True),
        StructField("reading_quality", StringType(), True),
        StructField("reading_time", StringType(), True),
        StructField("reading_value", StringType(), True),
        StructField("obis_code", StringType(), True),
        StructField("ansi_code", StringType(), True),
        StructField("service_multiplier", StringType(), True),
        StructField("dst_flag", StringType(), True),
        StructField("account_number", StringType(), True),
        StructField("source_quality_code", StringType(), True)]
    )
    s3path='s3://psegli-datalakeprodli-datalake-raw-prod/mdm-stage/'
    
    mdm_df = spark.read.format('csv').option("delimiter","|").options(header='true').schema(customSchema).load(s3path)
    mdm_df.show(10)

    ## Apply Filter to keep D - "Detail" meter records
    mdm_df = mdm_df.filter(mdm_df.record_type == 'D')

    ## Add partition column
    mdm_df = mdm_df.withColumn('reading_date', substring('reading_time', 1, 8)).withColumn("insert_date", lit(date.today())).repartition("reading_date")
    mdm_df = mdm_df.withColumn('reading_timestamp', to_timestamp(col("reading_time"), 'yyyyMMddHHmmss'))
    mdm_df = mdm_df.withColumn('reading_datestamp', to_date(col("reading_date"), 'yyyyMMdd'))
    mdm_df = mdm_df.withColumn("reading_type_desc", udf_hrf_reading_type(col('reading_type')))
    mdm_df = mdm_df.withColumn("reading_quality_desc", udf_hrf_estimate_actual(col('reading_quality')))
    mdm_df = mdm_df.withColumn("reading_value", col("reading_value").cast("double"))

    mdm_df = mdm_df.filter("reading_type_desc in ('KWH_REC','KWH_DEL')")

    # PySpark Write
    mdm_df.write.mode("overwrite").partitionBy('reading_date').format("parquet").save("s3://psegli-datalakeprodli-datalake-curated-prod/mdm/")
    sc.stop()
    