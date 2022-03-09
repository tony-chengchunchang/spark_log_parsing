from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, MapType
from device_detector import DeviceDetector

@F.udf(returnType=MapType(StringType(), StringType()))
def parse_qs(qs):
    pair_str_list = qs.split('&')
    qs_dict = dict([s.split('=') for s in pair_str_list])
    return qs_dict

@F.udf(returnType=MapType(StringType(), StringType()))
def parse_ua(ua):
    device = DeviceDetector(ua).parse()
    ua_dict = {
        'os_name': device.os_name(),
        'os_version': device.os_version(),
        'client_name': device.client_name(),
        'client_type': device.client_type(),
        'client_version': device.client_version(),
        'device_name': device.device_brand_name(),
        'device_type': device.device_type(),
        'device_model': device.device_model()
    }
    return ua_dict


spark = SparkSession.builder.getOrCreate()
df = spark.read.json('20210613_000.tar.gz')
df = df.drop(df.columns[0])
df = df.na.drop()

df = df.withColumn('qs_map', parse_qs('qs'))
df = df.withColumn('ua_map', parse_ua('user_agent'))
df.select('qs_map', 'ua_map').show()

spark.stop()