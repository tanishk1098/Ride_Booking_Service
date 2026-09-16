from pyspark import pipelines as dp
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Event Hubs configuration
EH_NAMESPACE = "UberEventsTani"
EH_NAME = "ubertopictani"
#Method 1 : configuration settings in the spark session
#Method 2: using databricks secrets using databricsk CLI commands
#method 3: dorectly using the connection string in the code (not recommended for production)

EH_CONN_STR = spark.conf.get("connection_string") #connection_string variable is stred in configuration!!
#EH_CONN_STR = dbutils.secrets.get(scope="ride-booking-secrets", key="connection_string")

# Kafka Consumer configuration

KAFKA_OPTIONS = {
  "kafka.bootstrap.servers"  : f"{EH_NAMESPACE}.servicebus.windows.net:9093",
  "subscribe"                : EH_NAME,
  "kafka.sasl.mechanism"     : "PLAIN",
  "kafka.security.protocol"  : "SASL_SSL",
  "kafka.sasl.jaas.config"   : f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{EH_CONN_STR}\";",
  "kafka.request.timeout.ms" : 10000,
  "kafka.session.timeout.ms" : 10000,
  "maxOffsetsPerTrigger"     : 10000,
  "failOnDataLoss"           : 'true',
  "startingOffsets"          : 'earliest'
}

#CREATED A STREAMING TABLE "rides_raw"  into uber catalog and bronze schema as defined in the configuration settings !!
@dp.table
def rides_raw():
 df=spark.readStream.format('kafka')\
  .options(**KAFKA_OPTIONS)\
  .load()
 df=df.withColumn("rides",col("value").cast(StringType()))
 return df
