
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "sampledb", table_name = "input_00", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "sampledb", table_name = "input_00", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("id", "string", "id", "string"), ("states", "string", "states", "string"), ("districts", "string", "districts", "string"), ("district_wise_covid_cases", "string", "district_wise_covid_cases", "string"), ("total_covid_cases", "long", "total_covid_cases", "long")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
#spark = SparkSession.builder.getOrCreate()
#df = spark.read.option("header",True).csv(r"C:/Users/shaiy/Documents/AWS/Session 3/sample_dataset_app.csv")
df = datasource0.toDF()
df.show()
df1 = df.withColumn('Districts',explode(split(df['Districts'],"\\|"))).withColumn('District_Wise_covid_cases',explode(split(df['District_Wise_covid_cases'],"\\|")))
datasource0 = DynamicFrame.fromDF(df1, glueContext, "datasource0")
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("id", "string", "id", "string"), ("states", "string", "states", "string"), ("districts", "string", "districts", "string"), ("district_wise_covid_cases", "string", "district_wise_covid_cases", "string"), ("total_covid_cases", "long", "total_covid_cases", "long")], transformation_ctx = "applymapping1")
## @type: SelectFields
## @args: [paths = ["id", "states", "districts", "district_wise_covid_cases", "total_covid_cases"], transformation_ctx = "selectfields2"]
## @return: selectfields2
## @inputs: [frame = applymapping1]
selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["id", "states", "districts", "district_wise_covid_cases", "total_covid_cases"], transformation_ctx = "selectfields2")
## @type: ResolveChoice
## @args: [choice = "MATCH_CATALOG", database = "sampledb", table_name = "input_00", transformation_ctx = "resolvechoice3"]
## @return: resolvechoice3
## @inputs: [frame = selectfields2]
resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "sampledb", table_name = "input_00", transformation_ctx = "resolvechoice3")
## @type: DataSink
## @args: [database = "sampledb", table_name = "input_00", transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = resolvechoice3]
datasink4 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice3, database = "sampledb", table_name = "input_00", transformation_ctx = "datasink4")
job.commit()