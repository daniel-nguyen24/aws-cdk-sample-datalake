import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from awsglue.dynamicframe import DynamicFrame

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Set up variables from passed args
args = getResolvedOptions(sys.argv, [
    'glue_src_db',
    'glue_src_tbl',
    'outputDir',
    'tempDir'
]
)
GLUE_SRC_DB = args['glue_src_db']
GLUE_SRC_TBL = args['glue_src_tbl']
GLUE_TMP_STORAGE = args['tempDir']
GLUE_OUTPUT_DIR = args['outputDir']


# Read from Glue table
wttrDf = glueContext.create_dynamic_frame.from_catalog(
    database=GLUE_SRC_DB, table_name=GLUE_SRC_TBL)
# wttrDf.printSchema()

# Relationalize to flatten the schema
dfc = wttrDf.relationalize('root', GLUE_TMP_STORAGE)
dfc.keys()

rootDf = dfc.select('root')
currentConditionDf = dfc.select('root_current_condition')
nearestAreaDf = dfc.select('root_nearest_area')

# Flatten current_condition
# Get weatherDesc
currentConditionWeatherDesc = dfc.select(
    'root_current_condition.val.weatherDesc')

# Join current_condition and current_condition.val.weatherDesc
flatCurrentConditionDf = Join.apply(currentConditionDf, currentConditionWeatherDesc, '`current_condition.val.weatherDesc`', 'id').drop_fields([
    '`current_condition.val.weatherDesc`', '`current_condition.val.weatherIconUrl`'])

# Flatten nearest_area
# Get areaName
nearestAreaAreaNameDf = dfc.select('root_nearest_area.val.areaName')

# Get country
nearestAreaCountryDf = dfc.select('root_nearest_area.val.country')

# Get region
nearestAreaRegionDf = dfc.select('root_nearest_area.val.region')

# Join everything together
flatNearestAreaDf = Join.apply(Join.apply(Join.apply(nearestAreaDf, nearestAreaAreaNameDf, '`nearest_area.val.areaName`', 'id'), nearestAreaCountryDf, '`nearest_area.val.country`', 'id'),
                               nearestAreaRegionDf, '`nearest_area.val.region`', 'id').drop_fields(['`nearest_area.val.areaName`', '`nearest_area.val.country`', '`nearest_area.val.region`', '`nearest_area.val.weatherUrl`'])

# Bring everything together to a flat df
# Create root df
rootDf = dfc.select('root')
rootDf = rootDf.drop_fields(['request', 'weather'])

# Join rootDf with flatCurrentConditionDf and flatNearestAreaDf
flatRootDf = Join.apply(Join.apply(rootDf, flatCurrentConditionDf, 'current_condition',
                        '`root_current_condition.id`'), flatNearestAreaDf, 'nearest_area', '`root_nearest_area.id`')
flatRootDf.printSchema()

cols_to_drop = [
    'current_condition',
    '`root_current_condition.id`',
    '`root_current_condition.index`',
    '`rootroot_current_conditionroot_current_condition.val.weatherDesc.id`',
    '`rootroot_current_conditionroot_current_condition.val.weatherDesc.index`',
    'nearest_area',
    '`root_nearest_area.index`',
    '`root_nearest_area.id`',
    '`root_nearest_arearoot_nearest_area.val.areaName.id`',
    '`root_nearest_arearoot_nearest_area.val.areaName.index`',
    '`root_nearest_arearoot_nearest_area.val.areaNameroot_nearest_area.val.country.id`',
    '`root_nearest_arearoot_nearest_area.val.areaNameroot_nearest_area.val.country.index`',
]


# Create timestamp column
flatRootDf = flatRootDf.drop_fields(cols_to_drop).toDF()
flatRootDf = flatRootDf.withColumn('localObsTimeStamp', to_timestamp(
    flatRootDf['`current_condition.val.localObsDateTime`'], 'yyyy-MM-dd h:mm a'))

flatRootDf = DynamicFrame.fromDF(flatRootDf, glueContext, name='curatedData')
result = flatRootDf = ApplyMapping.apply(frame=flatRootDf, mappings=[
    ("id", "`id`", "long"),
    ("index", "`index`", "int"),
    ("localObsTimeStamp", "`localObsTimeStamp`", "timestamp"),
    ("`current_condition.val.FeelsLikeC`", "FeelsLikeC", "int"),
    ("`current_condition.val.FeelsLikeF`", "FeelsLikeF", "int"),
    ("`current_condition.val.cloudcover`", "cloudcover", "int"),
    ("`current_condition.val.humidity`", "humidity", "int"),
    ("`current_condition.val.precipInches`", "precipInches", "float"),
    ("`current_condition.val.precipMM`", "precipMM", "float"),
    ("`current_condition.val.pressure`", "pressure", "int"),
    ("`current_condition.val.pressureInches`", "pressureInches", "int"),
    ("`current_condition.val.temp_C`", "temp_C", "int"),
    ("`current_condition.val.temp_F`", "temp_F", "int"),
    ("`current_condition.val.uvIndex`", "uvIndex", "int"),
    ("`current_condition.val.visibility`", "visibility", "int"),
    ("`current_condition.val.visibilityMiles`", "visibilityMiles", "int"),
    ("`current_condition.val.winddirDegree`", "winddirDegree", "int"),
    ("`current_condition.val.windspeedKmph`", "windspeedKmph", "int"),
    ("`current_condition.val.windspeedMiles`", "windspeedMiles", "int"),
    ("`nearest_area.val.areaName.val.value`", "areaName", "string"),
    ("`nearest_area.val.region.val.value`", "region", "string"),
    ("`nearest_area.val.country.val.value`", "country", "string"),
    ("`nearest_area.val.latitude`", "latitude", "float"),
    ("`nearest_area.val.longitude`", "longitude", "float"),
    ("`nearest_area.val.population`", "population", "long"),
])

# Write parquet files to s3 - partitioned by areaName
glueContext.write_dynamic_frame.from_options(
    frame=result,
    connection_type='s3',
    connection_options={
        'path': GLUE_OUTPUT_DIR,
        'partitionKeys': [
            'areaName'
        ]
    },
    format='parquet'
)
job.commit()
