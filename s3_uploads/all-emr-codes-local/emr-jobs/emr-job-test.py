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
    sc = SparkContext(appName="Curated_transaction")
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
    #id	customer_id	product_id	quantity	paid_at
    udf_hrf_estimate_actual = udf(hrf_estimate_actual)
        customSchema = StructType(
        [
        StructField("id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("paid_at", StringType(), True)
        ]
    )
    s3inputpath='s3://saidatech-datalake-raw-test/transaction-raw/'
    s3outputpath="s3://saidatech-datalake-outputdata-test/transaction-pivot/"
    
    transaction_df = spark.read.format('csv').option("delimiter","|").options(header='true').schema(customSchema).load(s3inputpath)
    transaction_df.show(10)
    transaction_df.groupBy("product_id").pivot("Country", countries).sum("quantity")
    transaction_df.groupBy("product_id").agg(sum("quantity"))


    ## Apply Filter to keep D - "Detail" meter records
    #transaction_df = transaction_df.filter(transaction_df.record_type == 'D')

    ## Add partition column
    # transaction_df = transaction_df.withColumn('reading_date', substring('reading_time', 1, 8)).withColumn("insert_date", lit(date.today())).repartition("reading_date")
    # transaction_df = transaction_df.withColumn('reading_timestamp', to_timestamp(col("reading_time"), 'yyyyMMddHHmmss'))
    # transaction_df = transaction_df.withColumn('reading_datestamp', to_date(col("reading_date"), 'yyyyMMdd'))
    # transaction_df = transaction_df.withColumn("reading_type_desc", udf_hrf_reading_type(col('reading_type')))
    # transaction_df = transaction_df.withColumn("reading_quality_desc", udf_hrf_estimate_actual(col('reading_quality')))
    # transaction_df = transaction_df.withColumn("reading_value", col("reading_value").cast("double"))

    # transaction_df = transaction_df.filter("reading_type_desc in ('KWH_REC','KWH_DEL')")

    # PySpark Write
    transaction_df.write.mode("overwrite").partitionBy('reading_date').format("parquet").save(s3outputpath)
    sc.stop()
    