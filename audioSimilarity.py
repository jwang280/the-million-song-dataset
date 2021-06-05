# -----------------------------------------------------------------------------
# Assignment 2
# Jiangwei Wang (19364744)
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# Processing Q1 (c)
# -----------------------------------------------------------------------------

# Counting rows in Spark

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------

# Python and pyspark modules required

import sys

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession, Row, DataFrame, Window, functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Required to allow the file to be submitted and run using spark-submit instead
# of using pyspark interactively

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()

# Compute suitable number of partitions

conf = sc.getConf()

N = int(conf.get("spark.executor.instances"))
M = int(conf.get("spark.executor.cores"))
partitions = 4 * N * M

# Load metadata from main into Spark to count unique song ids

df_metadata = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("codec", "gzip")
    .load("hdfs:///data/msd/main/summary/metadata.csv.gz/")
)

songCounts = df_metadata.select(
    F.count(F.col("song_id")).alias("songCount"),
    F.countDistinct(F.col("song_id")).alias("uniqueSongCount")
)
songCounts.show()

# +---------+---------------+
# |songCount|uniqueSongCount|
# +---------+---------------+
# |  1000000|         998963|
# +---------+---------------+

# load triplets dataset from tasteprofile into Spark to count unique song ids
df_triplets_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("song_id", StringType(), True),
    StructField("plays", IntegerType(), True)
])
df_triplets = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", "\t")
    .option("codec", "gzip")
    .schema(df_triplets_schema)
    .load("hdfs:///data/msd/tasteprofile/triplets.tsv/")
)

tripletsCounts = df_triplets.select(
    F.count(F.col("user_id")).alias("userCount"),
    F.countDistinct(F.col("user_id")).alias("uniqueUserCount"),
    F.countDistinct(F.col("song_id")).alias("uniqueSongCount")
)
tripletsCounts.show()

# +---------+---------------+---------------+
# |userCount|uniqueUserCount|uniqueSongCount|
# +---------+---------------+---------------+
# | 48373586|        1019318|         384546|
# +---------+---------------+---------------+

# -----------------------------------------------------------------------------
# Processing Q2 (a)
# -----------------------------------------------------------------------------

# define a schema for both of the mismatches and matches_manually_accepted datasets

mismatches_schema = StructType([
    StructField("song_id", StringType(), True),
    StructField("song_artist", StringType(), True),
    StructField("song_title", StringType(), True),
    StructField("track_id", StringType(), True),
    StructField("track_artist", StringType(), True),
    StructField("track_title", StringType(), True)
])

# load matches_manually_accepted into schema, show and count

with open("/scratch-network/courses/2020/DATA420-20S2/data/msd/tasteprofile/mismatches/sid_matches_manually_accepted.txt", "r") as f:
    lines = f.readlines()
    sid_matches_manually_accepted = []
    for line in lines:
        if line.startswith("< ERROR: "):
            a = line[10:28]
            b = line[29:47]
            c, d = line[49:-1].split("  !=  ")
            e, f = c.split("  -  ")
            g, h = d.split("  -  ")
            sid_matches_manually_accepted.append((a, e, f, b, g, h))

matches_manually_accepted = spark.createDataFrame(sc.parallelize(sid_matches_manually_accepted, 8), schema=mismatches_schema)
matches_manually_accepted.cache()
matches_manually_accepted.show(10, 30)

# +------------------+-----------------+--------------------+------------------+--------------------+--------------------+
# |           song_id|      song_artist|          song_title|          track_id|        track_artist|         track_title|
# +------------------+-----------------+--------------------+------------------+--------------------+--------------------+
# |SOFQHZM12A8C142342|     Josipa Lisac|             razloga|TRMWMFG128F92FFEF2|        Lisac Josipa|        1000 razloga|
# |SODXUTF12AB018A3DA|       Lutan Fyah|Nuh Matter the Cr...|TRMWPCD12903CCE5ED|             Midnite|Nah Matter the Cr...|
# |SOASCRF12A8C1372E6|Gaetano Donizetti|L'Elisir d'Amore:...|TRMHIPJ128F426A2E2|Gianandrea Gavazz...|L'Elisir D'Amore_...|
# |SOITDUN12A58A7AACA|     C.J. Chenier|           Ay, Ai Ai|TRMHXGK128F42446AB|     Clifton Chenier|           Ay_ Ai Ai|
# |SOLZXUM12AB018BE39|           許志安|            男人最痛|TRMRSOF12903CCF516|            Andy Hui|    Nan Ren Zui Tong|
# |SOTJTDT12A8C13A8A6|                S|                   h|TRMNKQE128F427C4D8|         Sammy Hagar|20th Century Man ...|
# |SOGCVWB12AB0184CE2|                H|                   Y|TRMUNCZ128F932A95D|            Hawkwind|25 Years (Alterna...|
# |SOKDKGD12AB0185E9C|     影山ヒロノブ|Cha-La Head-Cha-L...|TRMOOAH12903CB4B29|    Takahashi Hiroki|Maka fushigi adve...|
# |SOPPBXP12A8C141194|    Αντώνης Ρέμος|    O Trellos - Live|TRMXJDS128F42AE7CF|       Antonis Remos|           O Trellos|
# |SODQSLR12A8C133A01|    John Williams|Concerto No. 1 fo...|TRWHMXN128F426E03C|English Chamber O...|II. Andantino sic...|
# +------------------+-----------------+--------------------+------------------+--------------------+--------------------+

print(matches_manually_accepted.count())  

# 488

# load mismatches into schema, show and count

with open("/scratch-network/courses/2020/DATA420-20S2/data/msd/tasteprofile/mismatches/sid_mismatches.txt", "r") as f:
    lines = f.readlines()
    sid_mismatches = []
    for line in lines:
        if line.startswith("ERROR: "):
            a = line[8:26]
            b = line[27:45]
            c, d = line[47:-1].split("  !=  ")
            e, f = c.split("  -  ")
            g, h = d.split("  -  ")
            sid_mismatches.append((a, e, f, b, g, h))

mismatches = spark.createDataFrame(sc.parallelize(sid_mismatches, 64), schema=mismatches_schema)
mismatches.cache()
mismatches.show(10, 40)

# +------------------+-------------------+----------------------------------------+------------------+--------------+----------------------------------------+
# |           song_id|        song_artist|                              song_title|          track_id|  track_artist|                             track_title|
# +------------------+-------------------+----------------------------------------+------------------+--------------+----------------------------------------+
# |SOUMNSI12AB0182807|Digital Underground|                        The Way We Swing|TRMMGKQ128F9325E10|      Linkwood|           Whats up with the Underground|
# |SOCMRBE12AB018C546|         Jimmy Reed|The Sun Is Shining (Digitally Remaste...|TRMMREB12903CEB1B1|    Slim Harpo|               I Got Love If You Want It|
# |SOLPHZY12AC468ABA8|      Africa HiTech|                                Footstep|TRMMBOC12903CEB46E|Marcus Worgull|                 Drumstern (BONUS TRACK)|
# |SONGHTM12A8C1374EF|     Death in Vegas|                            Anita Berber|TRMMITP128F425D8D0|     Valen Hsu|                                  Shi Yi|
# |SONGXCA12A8C13E82E| Grupo Exterminador|                           El Triunfador|TRMMAYZ128F429ECE6|     I Ribelli|                               Lei M'Ama|
# |SOMBCRC12A67ADA435|      Fading Friend|                             Get us out!|TRMMNVU128EF343EED|     Masterboy|                      Feel The Heat 2000|
# |SOTDWDK12A8C13617B|       Daevid Allen|                              Past Lives|TRMMNCZ128F426FF0E| Bhimsen Joshi|            Raga - Shuddha Sarang_ Aalap|
# |SOEBURP12AB018C2FB|  Cristian Paduraru|                              Born Again|TRMMPBS12903CE90E1|     Yespiring|                          Journey Stages|
# |SOSRJHS12A6D4FDAA3|         Jeff Mills|                      Basic Human Design|TRMWMEL128F421DA68|           M&T|                           Drumsettester|
# |SOIYAAQ12A6D4F954A|           Excepter|                                      OG|TRMWHRI128F147EA8E|    The Fevers|Não Tenho Nada (Natchs Scheint Die So...|
# +------------------+-------------------+----------------------------------------+------------------+--------------+----------------------------------------+

print(mismatches.count())  

# 19094

# load triplets into schema and show

triplets_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("song_id", StringType(), True),
    StructField("plays", IntegerType(), True)
])
triplets = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", "\t")
    .option("codec", "gzip")
    .schema(triplets_schema)
    .load("hdfs:///data/msd/tasteprofile/triplets.tsv/")
    .cache()
)
triplets.show(10, 50)

# +--------------------+------------------+-----+
# |             user_id|           song_id|plays|
# +--------------------+------------------+-----+
# |f1bfc2a4597a3642f...|SOQEFDN12AB017C52B|    1|
# |f1bfc2a4597a3642f...|SOQOIUJ12A6701DAA7|    2|
# |f1bfc2a4597a3642f...|SOQOKKD12A6701F92E|    4|
# |f1bfc2a4597a3642f...|SOSDVHO12AB01882C7|    1|
# |f1bfc2a4597a3642f...|SOSKICX12A6701F932|    1|
# |f1bfc2a4597a3642f...|SOSNUPV12A8C13939B|    1|
# |f1bfc2a4597a3642f...|SOSVMII12A6701F92D|    1|
# |f1bfc2a4597a3642f...|SOTUNHI12B0B80AFE2|    1|
# |f1bfc2a4597a3642f...|SOTXLTZ12AB017C535|    1|
# |f1bfc2a4597a3642f...|SOTZDDX12A6701F935|    1|
# +--------------------+------------------+-----+

# left anti join to remove manualy corrected matches from mismatches dataset

mismatches_not_accepted = mismatches.join(matches_manually_accepted, on="song_id", how="left_anti")

# left anti join to remove mismatches from triplets dataset

triplets_not_mismatched = triplets.join(mismatches_not_accepted, on="song_id", how="left_anti")

# print left over mismatches row counts after we removed those manually accepted matched songs

print(mismatches_not_accepted.count())  

#19093

# print original and mismatch removed row counts

print(triplets.count())                 
print(triplets_not_mismatched.count())  

# 48373586
# 45795111


# -----------------------------------------------------------------------------
# Processing Q2 (b)
# -----------------------------------------------------------------------------

# define unique string from audio attributes directory's sencond columns, which are seperated by comma
# hdfs dfs -cat "/data/msd/audio/attributes/*" | awk -F',' '{print $2}' | sort | uniq

# NUMERIC
# real
# real 
# string
# string
# STRING

# assign each type from those columns to a correct corresponding data type
audio_attribute_type_mapping = {
  "NUMERIC": DoubleType(),
  "real": DoubleType(),
  "string": StringType(),
  "STRING": StringType()
}

# create a list of the shared prefixes from attributes and features directory csv file names
audio_dataset_names = [
  "msd-jmir-area-of-moments-all-v1.0",
  "msd-jmir-lpc-all-v1.0",
  "msd-jmir-methods-of-moments-all-v1.0",
  "msd-jmir-mfcc-all-v1.0",
  "msd-jmir-spectral-all-all-v1.0",
  "msd-jmir-spectral-derivatives-all-all-v1.0",
  "msd-marsyas-timbral-v1.0",
  "msd-mvd-v1.0",
  "msd-rh-v1.0",
  "msd-rp-v1.0",
  "msd-ssd-v1.0",
  "msd-trh-v1.0",
  "msd-tssd-v1.0"
]

# create a dictionay of key value pairs, which keys are the shared prefixes file names, values are schemas for 
# each corresponding dataset under feature directory, and rename each columns to a shorter but meaningful names
audio_dataset_schemas = {}
for audio_dataset_name in audio_dataset_names:
  print(audio_dataset_name)

  audio_dataset_path = f"/scratch-network/courses/2020/DATA420-20S2/data/msd/audio/attributes/{audio_dataset_name}.attributes.csv"
  with open(audio_dataset_path, "r") as f:
    rows = [line.strip().split(",") for line in f.readlines()]

    rows[-1][0] = "track_id"
    for i, row in enumerate(rows[0:-1]):
        row[0] = f"feature_{i:04d}"

  audio_dataset_schemas[audio_dataset_name] = StructType([
    StructField(row[0], audio_attribute_type_mapping[row[1]], True) for row in rows
  ])
    
  s = str(audio_dataset_schemas[audio_dataset_name])
  print(s[0:50] + " ... " + s[-50:])

# -----------------------------------------------------------------------------
# Audio Similarity Q1
# -----------------------------------------------------------------------------

# Find a small dataset to use

# hdfs dfs -du -h /data/msd/audio/features

# 65.5 M   524.2 M  /data/msd/audio/features/msd-jmir-area-of-moments-all-v1.0.csv
# 53.1 M   424.6 M  /data/msd/audio/features/msd-jmir-lpc-all-v1.0.csv
# 35.8 M   286.5 M  /data/msd/audio/features/msd-jmir-methods-of-moments-all-v1.0.csv
# 70.8 M   566.1 M  /data/msd/audio/features/msd-jmir-mfcc-all-v1.0.csv
# 51.1 M   408.9 M  /data/msd/audio/features/msd-jmir-spectral-all-all-v1.0.csv
# 51.1 M   408.9 M  /data/msd/audio/features/msd-jmir-spectral-derivatives-all-all-v1.0.csv
# 412.2 M  3.2 G    /data/msd/audio/features/msd-marsyas-timbral-v1.0.csv
# 1.3 G    10.3 G   /data/msd/audio/features/msd-mvd-v1.0.csv
# 240.3 M  1.9 G    /data/msd/audio/features/msd-rh-v1.0.csv
# 4.0 G    32.3 G   /data/msd/audio/features/msd-rp-v1.0.csv
# 640.6 M  5.0 G    /data/msd/audio/features/msd-ssd-v1.0.csv
# 1.4 G    11.5 G   /data/msd/audio/features/msd-trh-v1.0.csv
# 3.9 G    31.0 G   /data/msd/audio/features/msd-tssd-v1.0.csv

# -----------------------------------------------------------------------------
# Audio Similarity Q1 (a)
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# Load
# -----------------------------------------------------------------------------

features_schema = audio_dataset_schemas["msd-jmir-methods-of-moments-all-v1.0"]

audio_features = (
    spark.read.format("csv")
    .option("header", "false")
    .option("inferSchema", "true")
    .option("codec", "gzip")
    .option("quote", "\'")
    .schema(features_schema)
    .load("hdfs:///data/msd/audio/features/msd-jmir-methods-of-moments-all-v1.0.csv/")
    .repartition(partitions)
)
audio_features.cache().show(10, 50)

# +------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------------+
# |feature_0000|feature_0001|feature_0002|feature_0003|feature_0004|feature_0005|feature_0006|feature_0007|feature_0008|feature_0009|          track_id|
# +------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------------+
# |      0.2255|       11.41|       617.4|     38350.0|   5882000.0|      0.4831|       23.39|      1424.0|    156100.0|     2.675E7|TRWUEYI12903CFE5D6|
# |      0.2206|       8.848|       582.5|     56220.0|   9462000.0|      0.3483|       9.728|       511.2|     65970.0|     1.231E7|TRWFYQD128F4236DBF|
# |      0.2624|       7.957|       505.8|     34310.0|   4986000.0|      0.7082|       33.25|      1692.0|    111200.0|     1.406E7|TRWRPCZ12903C9CE26|
# |      0.2406|       14.43|       740.9|     50180.0|   7848000.0|      0.4814|       27.38|      1697.0|    164300.0|     2.724E7|TRMJCCX128F930CB29|
# |      0.1747|       10.46|       455.5|     23040.0|   3133000.0|      0.5926|        36.9|      2149.0|    196700.0|     3.277E7|TRMJTEY12903CB6BCE|
# |      0.2208|       12.23|       624.9|     34400.0|   4747000.0|      0.4315|       30.03|      1697.0|    175400.0|     3.018E7|TRGJZNT128F4280DFD|
# |     0.08287|       8.758|       520.9|     32540.0|   4733000.0|      0.1551|       24.79|      1406.0|    165400.0|     2.914E7|TRMSQJA128F9323A9E|
# |     0.07775|       12.22|       662.7|     45120.0|   6116000.0|      0.1307|       19.54|       936.5|     69710.0|   9130000.0|TRMEGXV128F9335451|
# |      0.2675|       21.64|       631.0|     42380.0|   5953000.0|      0.3507|        46.4|      2297.0|    166700.0|     2.769E7|TRWUBAB128F92F2BBA|
# |      0.2539|       14.72|       992.0|     69950.0|     1.148E7|      0.4091|        24.6|      1318.0|     97100.0|     1.392E7|TRGVYQB128F426B82A|
# +------------+------------+------------+------------+------------+------------+------------+------------+------------+------------+------------------+


print(audio_features.count()) # 994623

# -----------------------------------------------------------------------------
# Data analysis
# -----------------------------------------------------------------------------

# Imports

from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
import numpy as np

# Numeric feature distributions

statistics = (
    audio_features
    .select([col for col in audio_features.columns if col.startswith("f")])
    .describe()
    .toPandas()
    .set_index("summary")
    .rename_axis(None)
    .T
)
print(statistics)

#                count                 mean               stddev        min       max
# feature_0000  994623  0.15498176001746358  0.06646213086143019        0.0     0.959
# feature_0001  994623   10.384550576952279    3.868001393874675        0.0     55.42
# feature_0002  994623    526.8139724398088   180.43775499775236        0.0    2919.0
# feature_0003  994623    35071.97543290272   12806.816272955557        0.0  407100.0
# feature_0004  994623    5297870.369577217   2089356.4364558004        0.0   4.657E7
# feature_0005  994623  0.35084444325313185  0.18557956834383826        0.0     2.647
# feature_0006  994623   27.463867987840626    8.352648595163753        0.0     117.0
# feature_0007  994623   1495.8091812075531    505.8937639190225        0.0    5834.0
# feature_0008  994623   143165.46163257834    50494.27617103217  -146300.0  452500.0
# feature_0009  994623  2.396783048473542E7    9307340.299219687        0.0   9.477E7

# Save the descriptive statistics analysis figures as a csv file

spark.createDataFrame(statistics).toPandas().to_csv('statistics.csv')

# Correlations

assembler = VectorAssembler(
    inputCols=[col for col in audio_features.columns if col.startswith("f")],
    outputCol="Features"
)
features = assembler.transform(audio_features).select(["features", "track_id"])
features.cache()
features.count()

features.show(10, 100)

# +----------------------------------------------------------------------------+------------------+
# |                                                                    features|          track_id|
# +----------------------------------------------------------------------------+------------------+
# | [0.2028,11.56,700.6,40580.0,5525000.0,0.4144,27.39,1624.0,167200.0,2.829E7]|TRZEJUV128F42BA6C3|
# | [0.1809,9.726,522.3,31670.0,4726000.0,0.4986,28.07,1785.0,180300.0,2.948E7]|TRZCALK12903CEA516|
# | [0.1995,11.18,448.1,23990.0,2912000.0,0.3709,38.82,2276.0,193700.0,3.189E7]|TRBOHTW128F92D32E3|
# | [0.1493,7.203,322.0,13940.0,2532000.0,0.4902,38.06,2232.0,205500.0,3.391E7]|TRZIQUU128F422B648|
# | [0.2125,11.04,662.2,39320.0,5560000.0,0.3641,26.7,1026.0,58410.0,7054000.0]|TRFRXIS128F92E9E43|
# |   [0.2286,11.77,599.6,30380.0,4381000.0,0.465,32.17,1900.0,182900.0,3.07E7]|TRZWVQH128EF36376E|
# | [0.1899,7.022,383.4,22380.0,3462000.0,0.4209,30.87,1716.0,195000.0,3.396E7]|TRFGMDB128F42430A3|
# |  [0.158,8.525,421.8,16740.0,2595000.0,0.5852,35.41,2102.0,198300.0,3.268E7]|TRBSUKL12903CD3685|
# |[0.09735,7.524,337.5,21830.0,3665000.0,0.2207,28.22,1569.0,187900.0,3.368E7]|TRFGGKA128F1473B6B|
# | [0.1868,7.822,455.2,25650.0,3926000.0,0.4026,26.89,1710.0,184500.0,3.118E7]|TRFGAYH128F4260EBD|
# +----------------------------------------------------------------------------+------------------+


correlations = Correlation.corr(features, 'features', 'pearson').collect()[0][0].toArray()
print(correlations)

# [[ 1.          0.42628035  0.29630589  0.06103865 -0.05533585  0.75420787
#    0.49792898  0.44756461  0.16746557  0.10040744]
# [ 0.42628035  1.          0.85754866  0.60952091  0.43379677  0.02522827
#    0.40692287  0.39635353  0.01560657 -0.04090215]
# [ 0.29630589  0.85754866  1.          0.80300965  0.68290935 -0.08241507
#    0.12591025  0.18496247 -0.08817391 -0.13505636]
# [ 0.06103865  0.60952091  0.80300965  1.          0.94224443 -0.3276915
#   -0.22321966 -0.15823074 -0.24503392 -0.22087303]
# [-0.05533585  0.43379677  0.68290935  0.94224443  1.         -0.39255125
#   -0.35501874 -0.28596556 -0.26019779 -0.21181281]
# [ 0.75420787  0.02522827 -0.08241507 -0.3276915  -0.39255125  1.
#    0.54901522  0.5185027   0.34711201  0.2785128 ]
# [ 0.49792898  0.40692287  0.12591025 -0.22321966 -0.35501874  0.54901522
#    1.          0.90336675  0.51649906  0.4225494 ]
# [ 0.44756461  0.39635353  0.18496247 -0.15823074 -0.28596556  0.5185027
#    0.90336675  1.          0.7728069   0.68564528]
# [ 0.16746557  0.01560657 -0.08817391 -0.24503392 -0.26019779  0.34711201
#    0.51649906  0.7728069   1.          0.9848665 ]
# [ 0.10040744 -0.04090215 -0.13505636 -0.22087303 -0.21181281  0.2785128
#    0.4225494   0.68564528  0.9848665   1.        ]]

# Print strongly correlated column pairs, correlation coefficient larger than 0.95

# Set threshold

threshold = 0.95

for i in range(0, correlations.shape[0]):
    for j in range(i + 1, correlations.shape[1]):
        if correlations[i, j] > threshold:
            print((i, j))
            
# (8, 9)

num_correlated_columns_not_the_same = (correlations > threshold).sum() - correlations.shape[0]

print((correlations > threshold).astype(int))
print(correlations.shape) # (10, 10)
print(num_correlated_columns_not_the_same) # 2

# [[1 0 0 0 0 0 0 0 0 0]
 # [0 1 0 0 0 0 0 0 0 0]
 # [0 0 1 0 0 0 0 0 0 0]
 # [0 0 0 1 0 0 0 0 0 0]
 # [0 0 0 0 1 0 0 0 0 0]
 # [0 0 0 0 0 1 0 0 0 0]
 # [0 0 0 0 0 0 1 0 0 0]
 # [0 0 0 0 0 0 0 1 0 0]
 # [0 0 0 0 0 0 0 0 1 1]
 # [0 0 0 0 0 0 0 0 1 1]]

# Plot correlation heatmap

columns = [f"feature_000{i}" for i in range(0,10)]

import matplotlib.pyplot as plt
import seaborn as sns

plt.figure(figsize=(9,10))
correlation_plot = sns.heatmap(
    correlations,
    xticklabels=columns,
    yticklabels=columns,
    linecolor='white',
    linewidths=0.1,
    cmap="RdBu"
)
correlation_plot.set_title("Audio Features Correlations", pad=20, weight='bold', size=14)
plt.savefig('correlations.png')

# Remove one of each pair of correlated variables iteratively 

n = correlations.shape[0]
counter = 0
indexes = np.array(list(range(0, n)))
matrix = correlations
for j in range(0, n):
  mask = matrix > threshold
  sums = mask.sum(axis=0)
  index = np.argmax(sums)
  value = sums[index]
  check = value > 1
  if check:
    k = matrix.shape[0]
    keep = [i for i in range(0, k) if i != index]
    matrix = matrix[keep, :][:, keep]
    indexes = indexes[keep]
  else:
    break
  counter += 1
  print(counter) # 1


# Check correlations

correlations_new = correlations[indexes, :][:, indexes]
num_correlated_columns_not_the_same = (correlations_new > threshold).sum() - correlations_new.shape[0]

print((correlations_new > threshold).astype(int))
print(correlations_new.shape) # (9, 9)
print(num_correlated_columns_not_the_same) # 0

# Assemble vector only from the remaining columns

inputCols = np.array(audio_features.columns[:-1])[indexes]
assembler = VectorAssembler(
    inputCols=inputCols,
    outputCol="features"
).setHandleInvalid("skip")

features = assembler.transform(audio_features).select(["features", "track_id"])
features.cache()
features.show(10, 100)

# +-------------------------------------------------------------------+------------------+
# |                                                           features|          track_id|
# +-------------------------------------------------------------------+------------------+
# | [0.2028,11.56,700.6,40580.0,5525000.0,0.4144,27.39,1624.0,2.829E7]|TRZEJUV128F42BA6C3|
# | [0.1809,9.726,522.3,31670.0,4726000.0,0.4986,28.07,1785.0,2.948E7]|TRZCALK12903CEA516|
# | [0.1995,11.18,448.1,23990.0,2912000.0,0.3709,38.82,2276.0,3.189E7]|TRBOHTW128F92D32E3|
# | [0.1493,7.203,322.0,13940.0,2532000.0,0.4902,38.06,2232.0,3.391E7]|TRZIQUU128F422B648|
# |[0.2125,11.04,662.2,39320.0,5560000.0,0.3641,26.7,1026.0,7054000.0]|TRFRXIS128F92E9E43|
# |   [0.2286,11.77,599.6,30380.0,4381000.0,0.465,32.17,1900.0,3.07E7]|TRZWVQH128EF36376E|
# | [0.1899,7.022,383.4,22380.0,3462000.0,0.4209,30.87,1716.0,3.396E7]|TRFGMDB128F42430A3|
# |  [0.158,8.525,421.8,16740.0,2595000.0,0.5852,35.41,2102.0,3.268E7]|TRBSUKL12903CD3685|
# |[0.09735,7.524,337.5,21830.0,3665000.0,0.2207,28.22,1569.0,3.368E7]|TRFGGKA128F1473B6B|
# | [0.1868,7.822,455.2,25650.0,3926000.0,0.4026,26.89,1710.0,3.118E7]|TRFGAYH128F4260EBD|
# +-------------------------------------------------------------------+------------------+



# Check correlations

correlations = Correlation.corr(features, 'features', 'pearson').collect()[0][0].toArray()
num_correlated_columns_not_the_same = (correlations > threshold).sum() - correlations.shape[0]

print((correlations > threshold).astype(int))
print(correlations.shape) # (9, 9)
print(num_correlated_columns_not_the_same) # 0

# [[1 0 0 0 0 0 0 0 0]
 # [0 1 0 0 0 0 0 0 0]
 # [0 0 1 0 0 0 0 0 0]
 # [0 0 0 1 0 0 0 0 0]
 # [0 0 0 0 1 0 0 0 0]
 # [0 0 0 0 0 1 0 0 0]
 # [0 0 0 0 0 0 1 0 0]
 # [0 0 0 0 0 0 0 1 0]
 # [0 0 0 0 0 0 0 0 1]]

# -----------------------------------------------------------------------------
# Audio Similarity Q1 (b)
# -----------------------------------------------------------------------------

# Load the MSD All Music Genre Dataset

df_genre_schema = StructType([
    StructField("track_id", StringType(), True),
    StructField("genre", StringType(), True)
])
df_genre = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", "\t")
    .schema(df_genre_schema)
    .load("hdfs:///data/msd/genre/msd-MAGD-genreAssignment.tsv")
)
df_genre.cache().show(10,50)

# +------------------+--------------+
# |          track_id|         genre|
# +------------------+--------------+
# |TRAAAAK128F9318786|      Pop_Rock|
# |TRAAAAV128F421A322|      Pop_Rock|
# |TRAAAAW128F429D538|           Rap|
# |TRAAABD128F429CF47|      Pop_Rock|
# |TRAAACV128F423E09E|      Pop_Rock|
# |TRAAADT12903CCC339|Easy_Listening|
# |TRAAAED128E0783FAB|         Vocal|
# |TRAAAEF128F4273421|      Pop_Rock|
# |TRAAAEM128F93347B9|    Electronic|
# |TRAAAFD128F92F423A|      Pop_Rock|
# +------------------+--------------+

print(df_genre.count()) # 422714

# Anti join with mismatches_not_accepted, which we generatred before in Processing Q2 (a), to remove track id not matched with songs

mismatches_not_accepted.show(10,50)

# +------------------+-------------------+-----------------------------------------+------------------+--------------+-----------------------------------------+
# |           song_id|        song_artist|                               song_title|          track_id|  track_artist|                              track_title|
# +------------------+-------------------+-----------------------------------------+------------------+--------------+-----------------------------------------+
# |SOUMNSI12AB0182807|Digital Underground|                         The Way We Swing|TRMMGKQ128F9325E10|      Linkwood|            Whats up with the Underground|
# |SOCMRBE12AB018C546|         Jimmy Reed|The Sun Is Shining (Digitally Remastered)|TRMMREB12903CEB1B1|    Slim Harpo|                I Got Love If You Want It|
# |SOLPHZY12AC468ABA8|      Africa HiTech|                                 Footstep|TRMMBOC12903CEB46E|Marcus Worgull|                  Drumstern (BONUS TRACK)|
# |SONGHTM12A8C1374EF|     Death in Vegas|                             Anita Berber|TRMMITP128F425D8D0|     Valen Hsu|                                   Shi Yi|
# |SONGXCA12A8C13E82E| Grupo Exterminador|                            El Triunfador|TRMMAYZ128F429ECE6|     I Ribelli|                                Lei M'Ama|
# |SOMBCRC12A67ADA435|      Fading Friend|                              Get us out!|TRMMNVU128EF343EED|     Masterboy|                       Feel The Heat 2000|
# |SOTDWDK12A8C13617B|       Daevid Allen|                               Past Lives|TRMMNCZ128F426FF0E| Bhimsen Joshi|             Raga - Shuddha Sarang_ Aalap|
# |SOEBURP12AB018C2FB|  Cristian Paduraru|                               Born Again|TRMMPBS12903CE90E1|     Yespiring|                           Journey Stages|
# |SOSRJHS12A6D4FDAA3|         Jeff Mills|                       Basic Human Design|TRMWMEL128F421DA68|           M&T|                            Drumsettester|
# |SOIYAAQ12A6D4F954A|           Excepter|                                       OG|TRMWHRI128F147EA8E|    The Fevers|Não Tenho Nada (Natchs Scheint Die Sonne)|
# +------------------+-------------------+-----------------------------------------+------------------+--------------+-----------------------------------------+

matched_genre = (
    df_genre
    .join(
    mismatches_not_accepted, 
    on="track_id", 
    how="left_anti"
    )
)

matched_genre.cache().show(10,50)

# +------------------+--------------+
# |          track_id|         genre|
# +------------------+--------------+
# |TRAAAAK128F9318786|      Pop_Rock|
# |TRAAAAV128F421A322|      Pop_Rock|
# |TRAAAAW128F429D538|           Rap|
# |TRAAABD128F429CF47|      Pop_Rock|
# |TRAAACV128F423E09E|      Pop_Rock|
# |TRAAADT12903CCC339|Easy_Listening|
# |TRAAAED128E0783FAB|         Vocal|
# |TRAAAEF128F4273421|      Pop_Rock|
# |TRAAAEM128F93347B9|    Electronic|
# |TRAAAFD128F92F423A|      Pop_Rock|
# +------------------+--------------+

print(matched_genre.count()) # 415350

# Distribution of genres for the songs that were matched

genre_distribution = (
    matched_genre
    .groupBy("genre")
    .count()
    .orderBy(F.col('count').desc())
)
genre_distribution.show(21,20)

# +--------------+------+
# |         genre| count|
# +--------------+------+
# |      Pop_Rock|234107|
# |    Electronic| 40430|
# |           Rap| 20606|
# |          Jazz| 17673|
# |         Latin| 17475|
# | International| 14094|
# |           RnB| 13874|
# |       Country| 11492|
# |     Religious|  8754|
# |        Reggae|  6885|
# |         Blues|  6776|
# |         Vocal|  6076|
# |          Folk|  5777|
# |       New Age|  3935|
# | Comedy_Spoken|  2051|
# |        Stage |  1604|
# |Easy_Listening|  1533|
# |   Avant_Garde|  1000|
# |     Classical|   542|
# |      Children|   468|
# |       Holiday|   198|
# +--------------+------+

# Save the data frame as a csv file

genre_distribution.toPandas().to_csv('genre_distribution.csv')

# Visualize the distribution of genres

genre_plot = (
    genre_distribution
    .toPandas()
    .plot(x="genre", y="count", kind='barh', figsize=(14, 8), color='#86bf91', zorder=2, width=0.85)
)
genre_plot.set_xlabel("Number of Track ID Count", labelpad=20, weight='bold', size=14)
genre_plot.set_ylabel("Genre", labelpad=20, weight='bold', size=14)
genre_plot.set_title("Distribution of Genres for Matched Songs' Track ID", pad=20, weight='bold', size=16)
plt.savefig('genre_distribution.png')

# -----------------------------------------------------------------------------
# Audio Similarity Q1 (c)
# -----------------------------------------------------------------------------

# Join the audio features data frame and genre data frame
audioFeature_genre = (
    features
    .join(
    df_genre,
    on = "track_id",
    how = "inner"
  )
)
audioFeature_genre.cache().show(10, 100)

# +------------------+--------------------------------------------------------------------+----------+
# |          track_id|                                                            features|     genre|
# +------------------+--------------------------------------------------------------------+----------+
# |TRAAABD128F429CF47|[0.1308,9.587,459.9,27280.0,4303000.0,0.2474,26.02,1067.0,8281000.0]|  Pop_Rock|
# |TRAABPK128F424CFDB|   [0.1208,6.738,215.1,11890.0,2278000.0,0.4882,41.76,2164.0,3.79E7]|  Pop_Rock|
# |TRAACER128F4290F96|  [0.2838,8.995,429.5,31990.0,5272000.0,0.5388,28.29,1656.0,3.164E7]|  Pop_Rock|
# |TRAADYB128F92D7E73|   [0.1346,7.321,499.6,38460.0,5877000.0,0.2839,15.75,929.6,2.058E7]|      Jazz|
# |TRAAGHM128EF35CF8E|  [0.1563,9.959,502.8,26190.0,3660000.0,0.3835,28.24,1864.0,2.892E7]|Electronic|
# |TRAAGRV128F93526C0|  [0.1076,7.401,389.7,19350.0,2739000.0,0.4221,30.99,1861.0,3.166E7]|  Pop_Rock|
# |TRAAGTO128F1497E3C|   [0.1069,8.987,562.6,43100.0,7057000.0,0.1007,22.9,1346.0,2.738E7]|  Pop_Rock|
# |TRAAHAU128F9313A3D| [0.08485,9.031,445.9,23750.0,3274000.0,0.2583,35.59,2015.0,3.336E7]|  Pop_Rock|
# |TRAAHEG128E07861C3|  [0.1699,17.22,741.3,52440.0,8275000.0,0.2812,28.83,1671.0,2.695E7]|       Rap|
# |TRAAHZP12903CA25F4|  [0.1654,12.31,565.1,33100.0,5273000.0,0.1861,38.38,1962.0,3.355E7]|       Rap|
# +------------------+--------------------------------------------------------------------+----------+



print(audioFeature_genre.count()) # 420620 

# -----------------------------------------------------------------------------
# Audio Similarity Q2 (a)
# -----------------------------------------------------------------------------









# -----------------------------------------------------------------------------
# Audio Similarity Q2 (b)
# -----------------------------------------------------------------------------

# Conver the genre column into a column representing if the song is "Rap", to 1, or 0 otherwise

def is_rap(genres):
    if genres == "Rap":
        return 1
 
    return 0

is_rap_udf = F.udf(is_rap, IntegerType())

data = audioFeature_genre.withColumn(
    "is_rap",
    is_rap_udf(F.col('genre')
    )
)
data.cache().show(10,100)

# +------------------+--------------------------------------------------------------------+----------+------+
# |          track_id|                                                            features|     genre|is_rap|
# +------------------+--------------------------------------------------------------------+----------+------+
# |TRAAABD128F429CF47|[0.1308,9.587,459.9,27280.0,4303000.0,0.2474,26.02,1067.0,8281000.0]|  Pop_Rock|     0|
# |TRAABPK128F424CFDB|   [0.1208,6.738,215.1,11890.0,2278000.0,0.4882,41.76,2164.0,3.79E7]|  Pop_Rock|     0|
# |TRAACER128F4290F96|  [0.2838,8.995,429.5,31990.0,5272000.0,0.5388,28.29,1656.0,3.164E7]|  Pop_Rock|     0|
# |TRAADYB128F92D7E73|   [0.1346,7.321,499.6,38460.0,5877000.0,0.2839,15.75,929.6,2.058E7]|      Jazz|     0|
# |TRAAGHM128EF35CF8E|  [0.1563,9.959,502.8,26190.0,3660000.0,0.3835,28.24,1864.0,2.892E7]|Electronic|     0|
# |TRAAGRV128F93526C0|  [0.1076,7.401,389.7,19350.0,2739000.0,0.4221,30.99,1861.0,3.166E7]|  Pop_Rock|     0|
# |TRAAGTO128F1497E3C|   [0.1069,8.987,562.6,43100.0,7057000.0,0.1007,22.9,1346.0,2.738E7]|  Pop_Rock|     0|
# |TRAAHAU128F9313A3D| [0.08485,9.031,445.9,23750.0,3274000.0,0.2583,35.59,2015.0,3.336E7]|  Pop_Rock|     0|
# |TRAAHEG128E07861C3|  [0.1699,17.22,741.3,52440.0,8275000.0,0.2812,28.83,1671.0,2.695E7]|       Rap|     1|
# |TRAAHZP12903CA25F4|  [0.1654,12.31,565.1,33100.0,5273000.0,0.1861,38.38,1962.0,3.355E7]|       Rap|     1|
# +------------------+--------------------------------------------------------------------+----------+------+


# Check the class balance of the binary label

(
    data
    .groupBy("is_rap")
    .count()
    .show(2)
)

# +------+------+
# |is_rap| count|
# +------+------+
# |     1| 20899|
# |     0|399721|
# +------+------+

print(20899 / 399721)

# 0.05228396806772724

# -----------------------------------------------------------------------------
# Splitting
# -----------------------------------------------------------------------------

# Imports

from pyspark.sql.window import *
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler
from pyspark.ml import Pipeline

# Helper function to print class balance

def print_class_balance(data, name):
    N = data.count()
    counts = data.groupBy("is_rap").count().toPandas()
    counts["ratio"] = counts["count"] / N
    print(name)
    print(N)
    print(counts)
    print("")

    
# -----------------------------------------------------------------------------
# randomSplit (not stratified)
# -----------------------------------------------------------------------------

training, test = data.randomSplit([0.8, 0.2], seed=99)
training.cache()
test.cache()

print_class_balance(data, "features")
print_class_balance(training, "training")
print_class_balance(test, "test")

# features
# 420620
#    is_rap   count     ratio
# 0       0  399721  0.950314
# 1       1   20899  0.049686

# training
# 336776
#    is_rap   count     ratio
# 0       0  320046  0.950323
# 1       1   16730  0.049677

# test
# 83844
#    is_rap  count     ratio
# 0       0  79675  0.950277
# 1       1   4169  0.049723


#Center and scale the numeric feature

standard_scaler = StandardScaler(
    inputCol='features', 
    outputCol="Features"
)
pipeline = Pipeline(stages=[standard_scaler])

pipeline_model = pipeline.fit(training)

training = pipeline_model.transform(training).select(['track_id', 'features', 'is_rap'])
test = pipeline_model.transform(test).select(['track_id', 'features', 'is_rap'])

training.show(10,100)

# +------------------+----------------------------------------------------------------------------------------------------+------+
# |          track_id|                                                                                            features|is_rap|
# +------------------+----------------------------------------------------------------------------------------------------+------+
# |TRAAABD128F429CF47|[2.0230555241853283,2.6238868328106526,2.627507736405283,2.1314065626894094,2.0838786552495003,1....|     0|
# |TRAABPK128F424CFDB|[1.8683876706543399,1.8441378407716889,1.2289126203539387,0.9289744879170483,1.1032013889515133,2...|     0|
# |TRAACER128F4290F96|[4.3894736832094505,2.4618610682311277,2.4538259899675348,2.4994023438575588,2.553150887863204,2....|     0|
# |TRAADYB128F92D7E73|[2.081829308527104,2.003700375822133,2.8543223855361592,3.00490822584438,2.846143355078158,1.4914...|     0|
# |TRAAGRV128F93526C0|[1.6642261039934352,2.0255957494139607,2.226440019302324,1.5118298016143723,1.3264568061186106,2....|     0|
# |TRAAGTO128F1497E3C|[1.6533993542462657,2.4596715308719452,3.21425495216702,3.367434855275423,3.417599737414763,0.529...|     0|
# |TRAAHAU128F9313A3D|[1.3123567372104363,2.4717139863474507,2.5475227215984253,1.855605053661051,1.58554931844919,1.35...|     0|
# |TRAAHEG128E07861C3|[2.6278068314914926,4.712979165640913,4.235206534023128,4.097175958483601,4.007458952402885,1.477...|     1|
# |TRAAJJW12903CBDDCB|[3.506320239547507,4.072539488079954,3.3862227340017643,2.9674054710756517,2.212698483810125,2.21...|     0|
# |TRAAJKJ128F92FB44F|[0.5971725824831462,1.8802652071982044,2.3298492170169047,3.227580832283706,3.5347967243007448,0....|     0|
# +------------------+----------------------------------------------------------------------------------------------------+------+


# Imports

from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, LinearSVC, \
    DecisionTreeClassifier, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

# Create a function to show all binary metrics

def print_binary_metrics(predictions, labelCol="is_rap", predictionCol="prediction", rawPredictionCol="rawPrediction"):

    total = predictions.count()
    positive = predictions.filter((col(labelCol) == 1)).count()
    negative = predictions.filter((col(labelCol) == 0)).count()
    nP = predictions.filter((col(predictionCol) == 1)).count()
    nN = predictions.filter((col(predictionCol) == 0)).count()
    TP = predictions.filter((col(predictionCol) == 1) & (col(labelCol) == 1)).count()
    FP = predictions.filter((col(predictionCol) == 1) & (col(labelCol) == 0)).count()
    FN = predictions.filter((col(predictionCol) == 0) & (col(labelCol) == 1)).count()
    TN = predictions.filter((col(predictionCol) == 0) & (col(labelCol) == 0)).count()

    binary_evaluator = BinaryClassificationEvaluator(rawPredictionCol=rawPredictionCol, labelCol=labelCol, metricName="areaUnderROC")
    auroc = binary_evaluator.evaluate(predictions)

    print('actual total:    {}'.format(total))
    print('actual positive: {}'.format(positive))
    print('actual negative: {}'.format(negative))
    print('nP:              {}'.format(nP))
    print('nN:              {}'.format(nN))
    print('TP:              {}'.format(TP))
    print('FP:              {}'.format(FP))
    print('FN:              {}'.format(FN))
    print('TN:              {}'.format(TN))
    print('precision:       {}'.format(TP / (TP + FP)))
    print('recall:          {}'.format(TP / (TP + FN)))
    print('accuracy:        {}'.format((TP + TN) / total))
    print('auroc:           {}'.format(auroc))

    
lr = LogisticRegression(featuresCol='features', labelCol='is_rap')
lr_model = lr.fit(training)
predictions = lr_model.transform(test)
# predictions.cache()

print_binary_metrics(predictions)

# actual total:    83844
# actual positive: 4169
# actual negative: 79675
# nP:              336
# nN:              83508
# TP:              63
# FP:              273
# FN:              4106
# TN:              79402
# precision:       0.1875
# recall:          0.015111537538978172
# accuracy:        0.9477720528600735
# auroc:           0.8433018703125252

# -----------------------------------------------------------------------------
# Exact stratification using Window (multi-class variant in comments)
# -----------------------------------------------------------------------------

# Random

temp = (
    data
    .withColumn("id", monotonically_increasing_id())
    .withColumn("Random", rand())
    .withColumn(
        "Row",
        row_number()
        .over(
            Window
            .partitionBy("is_rap")
            .orderBy("Random")
        )
    )
)
temp.show(10,100)

# +------------------+-------------------------------------------------------------------+-----+------+------------+---------------------+---+
# |          track_id|                                                           features|genre|is_rap|          id|               Random|Row|
# +------------------+-------------------------------------------------------------------+-----+------+------------+---------------------+---+
# |TREFVXR12903CDD962|   [0.2166,13.2,623.3,35210.0,4415000.0,0.682,33.37,2089.0,3.004E7]|  Rap|     1|  8589938844|  5.35242214959597E-5|  1|
# |TRKQYYR128F933B32F| [0.2118,13.52,620.3,40910.0,5315000.0,0.3131,29.31,1363.0,1.094E7]|  Rap|     1| 94489291457| 7.659664687553747E-5|  2|
# |TRUNDKF128F92FB66A| [0.2678,19.48,764.5,47120.0,5669000.0,0.5174,37.75,2098.0,2.824E7]|  Rap|     1| 51539628241|1.4204454210697826E-4|  3|
# |TRUZFCR12903D074D1|[0.09875,15.42,589.5,35140.0,4901000.0,0.1489,37.09,2196.0,2.984E7]|  Rap|     1|120259105601| 3.055855290403642E-4|  4|
# |TRVUTKI128F9323063|  [0.1229,15.09,812.4,53020.0,8553000.0,0.1915,22.4,1262.0,2.348E7]|  Rap|     1|111669171901| 3.160107666939638E-4|  5|
# |TRPDIPQ12903CE0A6E| [0.2029,15.76,542.0,40180.0,4951000.0,0.4815,38.56,2152.0,3.036E7]|  Rap|     1|120259099670| 3.944877098160937E-4|  6|
# |TRHFVGG128F9314AF8|[0.322,14.38,614.7,30470.0,4176000.0,0.5686,31.93,1291.0,8135000.0]|  Rap|     1|103079222382|  4.52632679977083E-4|  7|
# |TRIKXSB128F933A73C| [0.2028,14.56,636.7,37890.0,5202000.0,0.3786,31.76,1934.0,2.904E7]|  Rap|     1| 60129550576| 4.650456573875861E-4|  8|
# |TROWDYS128F14776B6|  [0.3181,19.49,729.2,52110.0,8012000.0,0.3418,31.3,1620.0,2.772E7]|  Rap|     1|128849033987| 5.648224029014548E-4|  9|
# |TRFLSRU128F4288DD6|  [0.2263,12.53,678.4,44010.0,7046000.0,0.4693,24.39,1353.0,2.72E7]|  Rap|     1|103079220583| 5.815849605552703E-4| 10|
# +------------------+-------------------------------------------------------------------+-----+------+------------+---------------------+---+


training = temp.where(
    ((col("is_rap") == 0) & (col("Row") < 399721 * 0.8)) |
    ((col("is_rap") == 1) & (col("Row") < 20899 * 0.8))
)
training.cache()

test = temp.join(training, on="id", how="left_anti")
test.cache()

print_class_balance(data, "features")
print_class_balance(training, "training")
print_class_balance(test, "test")

# features
# 420620
#    is_rap   count     ratio
# 0       0  399721  0.950314
# 1       1   20899  0.049686

# training
# 336495
#    is_rap   count     ratio
# 0       0  319776  0.950314
# 1       1   16719  0.049686

# test
# 84125
#    is_rap  count     ratio
# 0       0  79945  0.950312
# 1       1   4180  0.049688

# Center and scale the numeric feature

training = pipeline_model.transform(training).select(['track_id', 'features', 'is_rap'])
test = pipeline_model.transform(test).select(['track_id', 'features', 'is_rap'])

lr = LogisticRegression(featuresCol='features', labelCol='is_rap')
lr_model = lr.fit(training)
predictions = lr_model.transform(test)
# predictions.cache()

print_binary_metrics(predictions)

# actual total:    84125
# actual positive: 4180
# actual negative: 79945
# nP:              356
# nN:              83769
# TP:              78
# FP:              278
# FN:              4102
# TN:              79667
# precision:       0.21910112359550563
# recall:          0.018660287081339714
# accuracy:        0.9479346210995543
# auroc:           0.8412473572590721

# -----------------------------------------------------------------------------
# Subsampling
# -----------------------------------------------------------------------------

# Randomly down sample the larger class by twice as large as the fewer class
training_downsampled = (
    training
    .withColumn("Random", rand())
    .where((col("is_rap") != 0) | ((col("is_rap") == 0) & (col("Random") < 2 * (20899 / 399721))))
)
training_downsampled.cache()

print_class_balance(training_downsampled, "training_downsampled")

# training_downsampled
# 50272
#    is_rap  count     ratio
# 0       0  33553  0.667429
# 1       1  16719  0.332571


lr = LogisticRegression(featuresCol='features', labelCol='is_rap')
lr_model = lr.fit(training_downsampled)
predictions = lr_model.transform(test)
# predictions.cache()

print_binary_metrics(predictions)

# actual total:    84125
# actual positive: 4180
# actual negative: 79945
# nP:              12003
# nN:              72122
# TP:              2544
# FP:              9459
# FN:              1636
# TN:              70486
# precision:       0.21194701324668833
# recall:          0.6086124401913876
# accuracy:        0.8681129271916791
# auroc:           0.8441770717966679


# -----------------------------------------------------------------------------
# Hybrid Sampling
# -----------------------------------------------------------------------------

# Randomly upsample the fewer class by exploding a vector of length betwen 0 and n for each row

ratio = 18
n = 20
p = ratio / n  # ratio < n such that probability < 1

def random_resample(x, n, p):
    # Can implement custom sampling logic per class,
    if x == 0:
        return [0]  # no sampling
    if x == 1:
        return list(range((np.sum(np.random.random(n) > p))))  # upsampling
    return []  # drop

random_resample_udf = udf(lambda x: random_resample(x, n, p), ArrayType(IntegerType()))

training_resampled = (
    training_downsampled
    .withColumn("Sample", random_resample_udf(col("is_rap")))
    .select(
        col("track_id"),
        col("features"),
        col("is_rap"),
        explode(col("Sample")).alias("Sample")
    )
    .drop("Sample")
)
training_resampled.cache()

print_class_balance(training_resampled, "training_resampled")

# training_resampled
# 66973
#    is_rap  count     ratio
# 0       0  33553  0.500993
# 1       1  33420  0.499007

lr = LogisticRegression(featuresCol='features', labelCol='is_rap')
lr_model = lr.fit(training_resampled)
predictions = lr_model.transform(test)
# predictions.cache()

print_binary_metrics(predictions)

# actual total:    84125
# actual positive: 4180
# actual negative: 79945
# nP:              20215
# nN:              63910
# TP:              3367
# FP:              16848
# FN:              813
# TN:              63097
# precision:       0.1665594855305466
# recall:          0.8055023923444976
# accuracy:        0.7900624071322437
# auroc:           0.8624680140443451


# -----------------------------------------------------------------------------
# Observation reweighting
# -----------------------------------------------------------------------------

training_weighted = (
    training
    .withColumn(
        "Weight",
        when(col("is_rap") == 0, 1.0)
        .when(col("is_rap") == 1, 20.0)
        .otherwise(1.0)
    )
)

weights = (
    training_weighted
    .groupBy("is_rap")
    .agg(
        collect_set(col("Weight")).alias("Weights")
    )
    .toPandas()
)
print(weights)

#    is_rap Weights
# 0       1  [20.0]
# 1       0   [1.0]

lr = LogisticRegression(featuresCol='features', labelCol='is_rap', weightCol="Weight")
lr_model = lr.fit(training_weighted)
predictions = lr_model.transform(test)
# predictions.cache()

print_binary_metrics(predictions)

# actual total:    84125
# actual positive: 4180
# actual negative: 79945
# nP:              22637
# nN:              61488
# TP:              3342
# FP:              19295
# FN:              838
# TN:              60650
# precision:       0.14763440385210055
# recall:          0.7995215311004785
# accuracy:        0.7606775631500743
# auroc:           0.844210447014858

# -----------------------------------------------------------------------------
# Change default threshold from 0.5 to 0.2
# -----------------------------------------------------------------------------

predictions = lr_model.transform(test, {lr_model.threshold: 0.2})
print_binary_metrics(predictions)

# actual total:    84125
# actual positive: 4180
# actual negative: 79945
# nP:              48601
# nN:              35524
# TP:              4005
# FP:              44596
# FN:              175
# TN:              35349
# precision:       0.08240571181662928
# recall:          0.9581339712918661
# accuracy:        0.4678038632986627
# auroc:           0.859059069019042

# -----------------------------------------------------------------------------
# Change default threshold from 0.5 to 0.7
# -----------------------------------------------------------------------------

predictions = lr_model.transform(test, {lr_model.threshold: 0.7})
print_binary_metrics(predictions)

# actual total:    84125
# actual positive: 4180
# actual negative: 79945
# nP:              10288
# nN:              73837
# TP:              2384
# FP:              7904
# FN:              1796
# TN:              72041
# precision:       0.2317262830482115
# recall:          0.570334928229665
# accuracy:        0.8846953937592867
# auroc:           0.8592566794575561


# -----------------------------------------------------------------------------
# Cross Validation with Logistic Regression
# -----------------------------------------------------------------------------

# Impot 

from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

# Define a evaluator fotr the cross validation

evaluator = binary_evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction", labelCol="is_rap", metricName="areaUnderROC")

# Train the model by using Logistic Regression

lr = LogisticRegression(featuresCol='features', labelCol='is_rap')

# Create ParamGrid for Cross Validation

paramGrid = (ParamGridBuilder()
             .addGrid(lr.regParam, [0.0, 0.01, 0.1]) # regularization parameter
             .addGrid(lr.elasticNetParam, [0.0, 0.1, 0.2]) # Elastic Net Parameter (Ridge = 0)
             .build())

# Create 5-fold CrossValidator

cv = CrossValidator(estimator=lr, \
                    estimatorParamMaps=paramGrid, \
                    evaluator=evaluator, \
                    numFolds=5)
cvModel = cv.fit(training_resampled)
predictions_cv = cvModel.transform(test)
# predictions_cv.cache()

# Evaluate best model

print_binary_metrics(predictions_cv)

# actual total:    84125
# actual positive: 4180
# actual negative: 79945
# nP:              20194
# nN:              63931
# TP:              3293
# FP:              16901
# FN:              887
# TN:              63044
# precision:       0.16306823809052193
# recall:          0.7877990430622009
# accuracy:        0.7885527488855869
# auroc:           0.8556478497028894


# Create 10-fold CrossValidator

cv = CrossValidator(estimator=lr, \
                    estimatorParamMaps=paramGrid, \
                    evaluator=evaluator, \
                    numFolds=10)
cvModel = cv.fit(training_resampled)
predictions_cv = cvModel.transform(test)
# predictions_cv.cache()

# Evaluate best model

print_binary_metrics(predictions_cv)

# actual total:    84125
# actual positive: 4180
# actual negative: 79945
# nP:              20193
# nN:              63932
# TP:              3293
# FP:              16900
# FN:              887
# TN:              63045
# precision:       0.16307631357401078
# recall:          0.7877990430622009
# accuracy:        0.7885646359583952
# auroc:           0.8556448602074226

# -----------------------------------------------------------------------------
# Train a model with Random Forest classifier
# -----------------------------------------------------------------------------

rf = RandomForestClassifier(featuresCol='features', labelCol='is_rap')
rf_model = rf.fit(training_resampled)
predictions_rf = rf_model.transform(test)
# predictions_rf.cache()

print_binary_metrics(predictions_rf)

# actual total:    84125
# actual positive: 4180
# actual negative: 79945
# nP:              23084
# nN:              61041
# TP:              3403
# FP:              19681
# FN:              777
# TN:              60264
# precision:       0.14741812510830013
# recall:          0.8141148325358851
# accuracy:        0.75681426448737
# auroc:           0.855572524292269

# -----------------------------------------------------------------------------
# Cross Validation with Random Forest
# -----------------------------------------------------------------------------

# Train the model by using Random Forest

rf = RandomForestClassifier(featuresCol='features', labelCol='is_rap')

# Create ParamGrid for Cross Validation

rf_paramGrid = (ParamGridBuilder()
             .addGrid(rf.numTrees, [100, 200]) 
             .addGrid(rf.maxDepth, [2, 5]) 
             .addGrid(rf.maxBins, [16, 32])
             .build())

# Create 5-fold CrossValidator

cv = CrossValidator(estimator=rf, \
                    estimatorParamMaps=rf_paramGrid, \
                    evaluator=evaluator, \
                    numFolds=5)
cvModel = cv.fit(training_resampled)
predictions_cv = cvModel.transform(test)
# predictions_cv.cache()

# Evaluate best model

print_binary_metrics(predictions_cv)

# actual total:    84125
# actual positive: 4180
# actual negative: 79945
# nP:              23515
# nN:              60610
# TP:              3432
# FP:              20083
# FN:              748
# TN:              59862
# precision:       0.14594939400382734
# recall:          0.8210526315789474
# accuracy:        0.7523803863298663
# auroc:           0.8573285925341615

# Create 10-fold CrossValidator

cv = CrossValidator(estimator=rf, \
                    estimatorParamMaps=rf_paramGrid, \
                    evaluator=evaluator, \
                    numFolds=10)
cvModel = cv.fit(training_resampled)
predictions_cv = cvModel.transform(test)
# predictions_cv.cache()

# Evaluate best model

print_binary_metrics(predictions_cv)

# actual total:    84125
# actual positive: 4180
# actual negative: 79945
# nP:              23515
# nN:              60610
# TP:              3432
# FP:              20083
# FN:              748
# TN:              59862
# precision:       0.14594939400382734
# recall:          0.8210526315789474
# accuracy:        0.7523803863298663
# auroc:           0.857328592534161

# -----------------------------------------------------------------------------
# Train a model with Gradient-Boosted Trees classifier
# -----------------------------------------------------------------------------

gbt = GBTClassifier(featuresCol='features', labelCol='is_rap')
gbt_model = gbt.fit(training_resampled)
predictions_gbt = gbt_model.transform(test)
# predictions_gbt.cache()

print_binary_metrics(predictions_gbt)

# actual total:    84125
# actual positive: 4180
# actual negative: 79945
# nP:              21651
# nN:              62474
# TP:              3394
# FP:              18257
# FN:              786
# TN:              61688
# precision:       0.15675950302526442
# recall:          0.8119617224880383
# accuracy:        0.7736344725111441
# auroc:           0.8699007331894749

# -----------------------------------------------------------------------------
# Cross Validation with Gradient-Boosted Trees classifier
# -----------------------------------------------------------------------------

# Train the model by using Linear Support Vector machine

gbt = GBTClassifier(featuresCol='features', labelCol='is_rap')

# Create ParamGrid for Cross Validation

gbt_paramGrid = (ParamGridBuilder()
                .addGrid(gbt.maxDepth, [2, 4, 8])
                .addGrid(gbt.maxBins, [24, 32, 48])
                .addGrid(gbt.stepSize, [0.01, 0.1])
                .build())

# Create 5-fold CrossValidator

cv = CrossValidator(estimator=gbt, \
                    estimatorParamMaps=gbt_paramGrid, \
                    evaluator=evaluator, \
                    numFolds=5)
cvModel = cv.fit(training_resampled)
predictions_cv = cvModel.transform(test)
# predictions_cv.cache()

# Evaluate best model

print_binary_metrics(predictions_cv)

# actual total:    84125
# actual positive: 4180
# actual negative: 79945
# nP:              19540
# nN:              64585
# TP:              3284
# FP:              16256
# FN:              896
# TN:              63689
# precision:       0.16806550665301945
# recall:          0.785645933014354
# accuracy:        0.796112927191679
# auroc:           0.8685101285243656

# Create 10-fold CrossValidator

cv = CrossValidator(estimator=gbt, \
                    estimatorParamMaps=gbt_paramGrid, \
                    evaluator=evaluator, \
                    numFolds=10)
cvModel = cv.fit(training_resampled)
predictions_cv = cvModel.transform(test)
# predictions_cv.cache()

# Evaluate best model

# print_binary_metrics(predictions_cv)

# actual total:    84125
# actual positive: 4180
# actual negative: 79945
# nP:              19430
# nN:              64695
# TP:              3294
# FP:              16136
# FN:              886
# TN:              63809
# precision:       0.16953165208440557
# recall:          0.7880382775119618
# accuracy:        0.7976582466567608
# auroc:           0.8709147302526453

# -----------------------------------------------------------------------------
# Train a model with Liner Support Vector Machine
# -----------------------------------------------------------------------------

svm = LinearSVC(featuresCol='features', labelCol='is_rap')
svm_model = svc.fit(training_resampled)
predictions_svm = svm_model.transform(test)
# predictions_svm.cache()

print_binary_metrics(predictions_svm)

# actual total:    84125
# actual positive: 4180
# actual negative: 79945
# nP:              21181
# nN:              62944
# TP:              3322
# FP:              17859
# FN:              858
# TN:              62086
# precision:       0.15683867617204097
# recall:          0.7947368421052632
# accuracy:        0.7775096582466567
# auroc:           0.854189378403394

# -----------------------------------------------------------------------------
# Cross Validation with Linear Support Vector Machine
# -----------------------------------------------------------------------------

# Train the model by using Linear Support Vector machine

svm = LinearSVC(featuresCol='features', labelCol='is_rap')

# Create ParamGrid for Cross Validation

svm_paramGrid = (ParamGridBuilder()
                .addGrid(svm.regParam, [0, 0.01, 0.1]) # regularization parameter
                .build())

# Create 5-fold CrossValidator

cv = CrossValidator(estimator=svm, \
                    estimatorParamMaps=svm_paramGrid, \
                    evaluator=evaluator, \
                    numFolds=5)
cvModel = cv.fit(training_resampled)
predictions_cv = cvModel.transform(test)
# predictions_cv.cache()

# Evaluate best model

print_binary_metrics(predictions_cv)

# actual total:    84125
# actual positive: 4180
# actual negative: 79945
# nP:              20517
# nN:              63608
# TP:              3295
# FP:              17222
# FN:              885
# TN:              62723
# precision:       0.16059852804990984
# recall:          0.7882775119617225
# accuracy:        0.7847607726597325
# auroc:           0.8545066689090394

# Create 10-fold CrossValidator

cv = CrossValidator(estimator=svm, \
                    estimatorParamMaps=svm_paramGrid, \
                    evaluator=evaluator, \
                    numFolds=10)
cvModel = cv.fit(training_resampled)
predictions_cv = cvModel.transform(test)
# predictions_cv.cache()

# Evaluate best model

print_binary_metrics(predictions_cv)

# actual total:    84125
# actual positive: 4180
# actual negative: 79945
# nP:              21421
# nN:              62704
# TP:              3393
# FP:              18028
# FN:              787
# TN:              61917
# precision:       0.15839596657485644
# recall:          0.8117224880382775
# accuracy:        0.7763447251114413
# auroc:           0.8578049517296722

# -----------------------------------------------------------------------------
# Audio Similarity Q4 (a)
# -----------------------------------------------------------------------------

from pyspark.ml.feature import OneHotEncoder, StringIndexer

label_stringIdx = StringIndexer(inputCol = "genre", outputCol = "label")

pipeline = Pipeline(stages=[label_stringIdx])

pipelineFit = pipeline.fit(audioFeature_genre)
dataset = pipelineFit.transform(audioFeature_genre)

dataset.cache().show(10,100)
# +------------------+--------------------------------------------------------------------+----------+-----+
# |          track_id|                                                            features|     genre|label|
# +------------------+--------------------------------------------------------------------+----------+-----+
# |TRAAABD128F429CF47|[0.1308,9.587,459.9,27280.0,4303000.0,0.2474,26.02,1067.0,8281000.0]|  Pop_Rock|  0.0|
# |TRAABPK128F424CFDB|   [0.1208,6.738,215.1,11890.0,2278000.0,0.4882,41.76,2164.0,3.79E7]|  Pop_Rock|  0.0|
# |TRAACER128F4290F96|  [0.2838,8.995,429.5,31990.0,5272000.0,0.5388,28.29,1656.0,3.164E7]|  Pop_Rock|  0.0|
# |TRAADYB128F92D7E73|   [0.1346,7.321,499.6,38460.0,5877000.0,0.2839,15.75,929.6,2.058E7]|      Jazz|  3.0|
# |TRAAGHM128EF35CF8E|  [0.1563,9.959,502.8,26190.0,3660000.0,0.3835,28.24,1864.0,2.892E7]|Electronic|  1.0|
# |TRAAGRV128F93526C0|  [0.1076,7.401,389.7,19350.0,2739000.0,0.4221,30.99,1861.0,3.166E7]|  Pop_Rock|  0.0|
# |TRAAGTO128F1497E3C|   [0.1069,8.987,562.6,43100.0,7057000.0,0.1007,22.9,1346.0,2.738E7]|  Pop_Rock|  0.0|
# |TRAAHAU128F9313A3D| [0.08485,9.031,445.9,23750.0,3274000.0,0.2583,35.59,2015.0,3.336E7]|  Pop_Rock|  0.0|
# |TRAAHEG128E07861C3|  [0.1699,17.22,741.3,52440.0,8275000.0,0.2812,28.83,1671.0,2.695E7]|       Rap|  2.0|
# |TRAAHZP12903CA25F4|  [0.1654,12.31,565.1,33100.0,5273000.0,0.1861,38.38,1962.0,3.355E7]|       Rap|  2.0|
# +------------------+--------------------------------------------------------------------+----------+-----+

# Display multipleclass labeling detail
multiclass = (
    dataset
    .groupBy("genre")
    .agg(
        collect_set(col("label")).alias("label")
    )
    .orderBy("label")
    .toPandas()
)

# Save to csv file and print

multiclass.to_csv('multiclass.csv')
print(multiclass)

#              genre   label
# 0         Pop_Rock   [0.0]
# 1       Electronic   [1.0]
# 2              Rap   [2.0]
# 3             Jazz   [3.0]
# 4            Latin   [4.0]
# 5              RnB   [5.0]
# 6    International   [6.0]
# 7          Country   [7.0]
# 8        Religious   [8.0]
# 9           Reggae   [9.0]
# 10           Blues  [10.0]
# 11           Vocal  [11.0]
# 12            Folk  [12.0]
# 13         New Age  [13.0]
# 14   Comedy_Spoken  [14.0]
# 15          Stage   [15.0]
# 16  Easy_Listening  [16.0]
# 17     Avant_Garde  [17.0]
# 18       Classical  [18.0]
# 19        Children  [19.0]
# 20         Holiday  [20.0]

# Randomly split the dataset

training, test = dataset.randomSplit([0.8, 0.2], seed=99)
training.cache()
test.cache()

# Helper function to print class balance

def print_class_balance(data, name):
    N = data.count()
    counts = data.groupBy("label").count().toPandas()
    counts["ratio"] = counts["count"] / N
    print(name)
    print(N)
    print(counts)
    print("")
    
print_class_balance(dataset, "genres")
print_class_balance(training, "training")
print_class_balance(test, "test")

# genres
# 420620
#     label   count     ratio
# 0    12.0    5789  0.013763
# 1     6.0   14194  0.033745
# 2    13.0    4000  0.009510
# 3     1.0   40666  0.096681
# 4    10.0    6801  0.016169
# 5    16.0    1535  0.003649
# 6    19.0     463  0.001101
# 7     5.0   14314  0.034031
# 8     7.0   11691  0.027795
# 9     4.0   17504  0.041615
# 10   17.0    1012  0.002406
# 11   20.0     200  0.000475
# 12    8.0    8780  0.020874
# 13    9.0    6931  0.016478
# 14   14.0    2067  0.004914
# 15    0.0  237649  0.564997
# 16   18.0     555  0.001319
# 17    3.0   17775  0.042259
# 18    2.0   20899  0.049686
# 19   15.0    1613  0.003835
# 20   11.0    6182  0.014697


# training
# 336199
#     label   count     ratio
# 0    12.0    4623  0.013751
# 1     6.0   11333  0.033709
# 2    13.0    3182  0.009465
# 3     1.0   32494  0.096651
# 4    10.0    5473  0.016279
# 5    16.0    1223  0.003638
# 6    19.0     375  0.001115
# 7     5.0   11373  0.033828
# 8     7.0    9340  0.027781
# 9     4.0   14069  0.041847
# 10   17.0     816  0.002427
# 11   20.0     160  0.000476
# 12    8.0    6964  0.020714
# 13    9.0    5542  0.016484
# 14   14.0    1643  0.004887
# 15    0.0  189983  0.565091
# 16   18.0     436  0.001297
# 17    3.0   14225  0.042311
# 18    2.0   16730  0.049762
# 19   11.0    4929  0.014661
# 20   15.0    1286  0.003825


# test
# 84421
#     label  count     ratio
# 0     6.0   2861  0.033890
# 1    13.0    818  0.009690
# 2    12.0   1166  0.013812
# 3     1.0   8172  0.096801
# 4    10.0   1328  0.015731
# 5    16.0    312  0.003696
# 6    19.0     88  0.001042
# 7     5.0   2941  0.034837
# 8     7.0   2351  0.027849
# 9     4.0   3435  0.040689
# 10   17.0    196  0.002322
# 11   20.0     40  0.000474
# 12    9.0   1389  0.016453
# 13    8.0   1816  0.021511
# 14   14.0    424  0.005022
# 15    0.0  47666  0.564623
# 16   18.0    119  0.001410
# 17    3.0   3550  0.042051
# 18    2.0   4169  0.049383
# 19   15.0    327  0.003873
# 20   11.0   1253  0.014842


# -----------------------------------------------------------------------------
# Assemble, center and scale the numeric predictors
# -----------------------------------------------------------------------------

assembler = VectorAssembler(
    inputCols=[col for col in training.columns if col.startswith("f")],
    outputCol="assembled"
)
standard_scaler = StandardScaler(
    inputCol="features", 
    outputCol="Features"
)
pipeline = Pipeline(stages=[standard_scaler])

pipeline_model = pipeline.fit(training)


training = pipeline_model.transform(training).select(['track_id', 'features', 'genre', 'label'])
test = pipeline_model.transform(test).select(['track_id', 'features', 'genre', 'label'])
training.show(10,100)

# +------------------+----------------------------------------------------------------------------------------------------+-------------+-----+
# |          track_id|                                                                                            features|        genre|label|
# +------------------+----------------------------------------------------------------------------------------------------+-------------+-----+
# |TRAAABD128F429CF47|[2.0230555241853283,2.6238868328106526,2.627507736405283,2.13140656268941,2.0838786552495008,1.29...|     Pop_Rock|  0.0|
# |TRAABPK128F424CFDB|[1.8683876706543399,1.8441378407716889,1.2289126203539387,0.9289744879170485,1.1032013889515135,2...|     Pop_Rock|  0.0|
# |TRAACER128F4290F96|[4.3894736832094505,2.4618610682311277,2.4538259899675348,2.499402343857559,2.5531508878632043,2....|     Pop_Rock|  0.0|
# |TRAADYB128F92D7E73|[2.081829308527104,2.003700375822133,2.8543223855361592,3.0049082258443804,2.8461433550781585,1.4...|         Jazz|  3.0|
# |TRAAGRV128F93526C0|[1.6642261039934352,2.0255957494139607,2.226440019302324,1.5118298016143725,1.326456806118611,2.2...|     Pop_Rock|  0.0|
# |TRAAGTO128F1497E3C|[1.6533993542462657,2.4596715308719452,3.21425495216702,3.367434855275424,3.4175997374147635,0.52...|     Pop_Rock|  0.0|
# |TRAAHAU128F9313A3D|[1.3123567372104363,2.4717139863474507,2.5475227215984253,1.8556050536610516,1.5855493184491904,1...|     Pop_Rock|  0.0|
# |TRAAHEG128E07861C3|[2.6278068314914926,4.712979165640913,4.235206534023128,4.097175958483602,4.007458952402886,1.477...|          Rap|  2.0|
# |TRAAJJW12903CBDDCB|[3.506320239547507,4.072539488079954,3.3862227340017643,2.967405471075652,2.2126984838101253,2.21...|International|  6.0|
# |TRAAJKJ128F92FB44F|[0.5971725824831462,1.8802652071982044,2.3298492170169047,3.227580832283707,3.534796724300745,0.2...|         Folk| 12.0|
# +------------------+----------------------------------------------------------------------------------------------------+-------------+-----+



# Create a function to print all multi-class classification performance evaluation metrics

def print_multi_metrics(predictions, predictionCol="prediction", labelCol="label"):

    total = predictions.count()
    
    multi_evaluator_f1 = MulticlassClassificationEvaluator(labelCol=labelCol, metricName="f1")
    f1 = multi_evaluator_f1.evaluate(predictions)
    multi_evaluator_accuracy = MulticlassClassificationEvaluator(labelCol=labelCol, metricName="accuracy")
    accuracy = multi_evaluator_accuracy.evaluate(predictions)
    multi_evaluator_weightedPrecision = MulticlassClassificationEvaluator(labelCol=labelCol, metricName="weightedPrecision")
    weightedPrecision = multi_evaluator_weightedPrecision.evaluate(predictions)
    multi_evaluator_weightedRecall = MulticlassClassificationEvaluator(labelCol=labelCol, metricName="weightedRecall")
    weightedRecall = multi_evaluator_weightedRecall.evaluate(predictions)
    
    print('actual total:      {}'.format(total))
    print('f1:                {}'.format(f1))
    print('accuracy:          {}'.format(accuracy))
    print('weightedPrecision: {}'.format(weightedPrecision))
    print('weightedRecall:    {}'.format(weightedRecall))

# Define and fit the model with logistic regression method

lr = LogisticRegression(featuresCol='features', labelCol='label')
lr_model = lr.fit(training)
predictions = lr_model.transform(test)
predictions.cache()

# Evaluate the model performance

print_multi_metrics(predictions)

# actual total:      84421
# f1:                0.4506128351809822
# accuracy:          0.5729024768718683
# weightedPrecision: 0.4086876472226013
# weightedRecall:    0.5729024768718686


def print_class_prediction(data, name):
    N = data.count()
    counts = data.groupBy("prediction").count().toPandas()
    counts["ratio"] = counts["count"] / N
    print(name)
    print(N)
    print(counts)
    print("")

print_class_prediction(predictions, "predictions")

# predictions
# 84421
#    prediction  count     ratio
# 0         1.0   3191  0.037799
# 1         5.0    171  0.002026
# 2         9.0      5  0.000059
# 3        14.0      6  0.000071
# 4         0.0  78503  0.929899
# 5         3.0    542  0.006420
# 6         2.0   1994  0.023620
# 7        15.0      9  0.000107


# List each classes' precision scores

lr_model.summary.precisionByLabel

# [0.5879762588550641,
 # 0.4004070004070004,
 # 0.3583966465810846,
 # 0.3172067549064354,
 # 0.0,
 # 0.23604465709728867,
 # 0.0,
 # 0.0,
 # 0.0,
 # 0.08,
 # 0.0,
 # 0.0,
 # 0.0,
 # 0.0,
 # 0.0,
 # 0.0,
 # 0.0,
 # 0.0,
 # 0.0,
 # 0.0,
 # 0.0]

# List all classes' recall scores

lr_model.summary.recallByLabel

# [0.9698762520857129,
 # 0.15138179356188836,
 # 0.16353855349671248,
 # 0.04885764499121265,
 # 0.0,
 # 0.013013277059702805,
 # 0.0,
 # 0.0,
 # 0.0,
 # 0.00036088054853843375,
 # 0.0,
 # 0.0,
 # 0.0,
 # 0.0,
 # 0.0,
 # 0.0,
 # 0.0,
 # 0.0,
 # 0.0,
 # 0.0,
 # 0.0]

paramGrid = (ParamGridBuilder()
             .addGrid(lr.regParam, [0.0, 0.01, 0.1]) # regularization parameter
             .addGrid(lr.elasticNetParam, [0.0, 0.1, 0.2]) # Elastic Net Parameter (Ridge = 0)
             .build())

# Create 5-fold CrossValidator

multi_evaluator = MulticlassClassificationEvaluator(labelCol="label", metricName="f1")

cv = CrossValidator(estimator=lr, \
                    estimatorParamMaps=paramGrid, \
                    evaluator=multi_evaluator, \
                    numFolds=5)
cvModel = cv.fit(training)
predictions_cv = cvModel.transform(test)

print_multi_metrics(predictions_cv)

# actual total:      84421
# f1:                0.4506128351809822
# accuracy:          0.5729024768718683
# weightedPrecision: 0.4086876472226013
# weightedRecall:    0.5729024768718686


print_class_prediction(predictions_cv, "predictions")

#    prediction  count     ratio
# 0         1.0   3191  0.037799
# 1         5.0    171  0.002026
# 2         9.0      5  0.000059
# 3        14.0      6  0.000071
# 4         0.0  78503  0.929899
# 5         3.0    542  0.006420
# 6         2.0   1994  0.023620
# 7        15.0      9  0.000107


rf = RandomForestClassifier(featuresCol='features', labelCol='label')
rf_model = rf.fit(training)
predictions_rf = rf_model.transform(test)

print_multi_metrics(predictions_rf)

# actual total:      84421
# f1:                0.42500618040464955
# accuracy:          0.569905592210469
# weightedPrecision: 0.37092492168711966
# weightedRecall:    0.569905592210469

print_class_prediction(predictions_rf, "predictions")

# predictions
# 84421
#    prediction  count     ratio
# 0         1.0   1560  0.018479
# 1         0.0  82861  0.981521




