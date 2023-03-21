import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import json

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# BEGINNING ----------
from pyspark.sql.types import *
from pyspark.sql.functions import *

historical_df = spark.read\
                .parquet("s3a://aws-glue-assets-497037598026-us-east-1/data_historica_pronostico_municipal/pronostico_municipio")

valor_mayor_fechahora_carga = historical_df.select(max(col("fechahora_carga"))).first()[0]
df_api_municipios = historical_df\
                    .filter(col("fechahora_carga") == valor_mayor_fechahora_carga)\

historical_df_with_old_flag = historical_df\
                .select(col("idmun").alias("idmun_old"), col("ides").alias("ides_old"), col("dloc").alias("dloc_old"),
                    col("tmax").alias("tmax_old"), col("tmin").alias("tmin_old"), col("prec").alias("prec_old"),
                    col("dh").alias("dh_old"), col("fechahora_carga").alias("fechahora_carga_old"))


def match_con_data(actual, previo, delta_min):
    min_a_restar1 = str(60 + delta_min)
    min_a_restar2 = str(60 - delta_min)
    df_1h_menos = actual.withColumn("fechahora_carga_max",
                                    col("fechahora_carga") - expr("INTERVAL " + min_a_restar2 + " MINUTES")) \
        .withColumn("fechahora_carga_min", col("fechahora_carga") - expr("INTERVAL " + min_a_restar1 + " MINUTES"))
    cond = [
        df_1h_menos["idmun"] == previo["idmun_old"],
        df_1h_menos["ides"] == previo["ides_old"],
        df_1h_menos["dloc"] == previo["dloc_old"],
        col("fechahora_carga_min") <= col("fechahora_carga_old"),
        col("fechahora_carga_old") <= col("fechahora_carga_max")
    ]
    df_filtered = df_1h_menos.join(previo, cond, "left")
    return df_filtered.drop("fechahora_carga_min", "fechahora_carga_max")


df_matched = match_con_data(df_api_municipios, historical_df_with_old_flag, 15).drop("idmun_old", "ides_old")
df_columns_to_parse = ["tmin", "tmax", 'prec', "tmax_old", "tmin_old", 'prec_old']
df_normal_columns = ['idmun', 'ides', 'nes', 'nmun', 'dloc', 'dh', 'fechahora_carga', 'fechahora_carga_old']
parsed_df = df_matched.select(*(col(c).cast("float") for c in df_columns_to_parse),
                              *(col(c) for c in df_normal_columns))

import builtins

avg_double = udf(lambda array: builtins.sum(array) / len(array), DoubleType())
df_to_db = parsed_df \
    .fillna(float('-inf'), subset=["tmax_old", "tmin_old", "prec_old"]) \
    .withColumn(
    "prom_tmax_ult_2h",
    round(when(col("tmax_old") == float('-inf'), col("tmax")).otherwise(avg_double(array("tmax", "tmax_old"))), 2)
) \
    .withColumn(
    "prom_tmin_ult_2h",
    round(when(col("tmin_old") == float('-inf'), col("tmin")).otherwise(avg_double(array("tmin", "tmin_old"))), 2)
) \
    .withColumn(
    "prom_prec_ult_2h",
    round(when(col("prec_old") == float('-inf'), col("prec")).otherwise(avg_double(array("prec", "prec_old"))), 2)
) \
    .drop("tmin", "tmax", "prec", "tmax_old", "tmin_old", "prec_old", "fechahora_carga_old")

from awsglue.dynamicframe import DynamicFrame
dyn_frame = DynamicFrame.fromDF(df_to_db, glueContext, "dyn_frame_to_db")

my_conn_options = {
    "dbtable": "miesquema.pronosticomunicipioultimas2h",
    "database": "pronosticomunicipio",
}

glueContext.write_dynamic_frame.from_jdbc_conf(
    frame = dyn_frame,
    catalog_connection = "redshift_database",
    connection_options = my_conn_options,
    redshift_tmp_dir = "s3://aws-glue-assets-497037598026-us-east-1/redshift-temp_folder/"
) 


# DBTITLE 1,Redshift
def read_file(file_location):
    file_type = "csv"
    infer_schema = "true"
    first_row_is_header = "true"
    delimiter = ","
    df = spark.read.format(file_type) \
        .option("inferSchema", infer_schema) \
        .option("header", first_row_is_header) \
        .option("sep", delimiter) \
        .load(file_location)
    return df


def get_upload_date(df, pattern):
    filename_column = "filename"
    df_with_upload_date = df \
        .withColumn(filename_column, reverse(split(input_file_name(), pattern))[0]) \
        .withColumn("upload_date", substring(col(filename_column), 0, 8)) \
        .drop(filename_column)
    return df_with_upload_date


data_municipios_prev = read_file("s3://aws-glue-assets-497037598026-us-east-1/data_municipios/*/data.csv")
data_municipios = get_upload_date(data_municipios_prev, "data_municipios/")
valor_mayor_upload_date = data_municipios.select(max(col("upload_date"))).first()[0]
ultimo_data_municipios = data_municipios \
    .filter(col("upload_date") == valor_mayor_upload_date)\
    .drop("upload_date")

cond = [
    df_to_db["idmun"] == ultimo_data_municipios["Cve_Mun"],
    df_to_db["ides"] == ultimo_data_municipios["Cve_Ent"],
]
df_redshift = ultimo_data_municipios.join(df_to_db, cond, "inner")\
                .drop("Cve_Ent", "Cve_Mun")

dyn_frame_redshift = DynamicFrame.fromDF(df_redshift, glueContext, "dyn_frame_to_redshift")

my_conn_options = {
    "dbtable": "miesquema.pronosticomunicipio_current",
    "database": "pronosticomunicipio",
}

glueContext.write_dynamic_frame.from_jdbc_conf(
    frame = dyn_frame_redshift,
    catalog_connection = "redshift_database",
    connection_options = my_conn_options,
    redshift_tmp_dir = "s3://aws-glue-assets-497037598026-us-east-1/redshift-temp_folder/"
)
# END ----------


job.init(args['JOB_NAME'], args)
job.commit()