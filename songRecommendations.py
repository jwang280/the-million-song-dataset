# -----------------------------------------------------------------------------
# Assignment 2
# Jiangwei Wang (19364744)
# -----------------------------------------------------------------------------

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

# -----------------------------------------------------------------------------
# Song Recommendations Q1 (a)
# -----------------------------------------------------------------------------


# -----------------------------------------------------------------------------
# Load
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
matches_manually_accepted.show(10, 20)

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
mismatches.show(10, 20)

# +------------------+-------------------+--------------------+------------------+--------------+--------------------+
# |           song_id|        song_artist|          song_title|          track_id|  track_artist|         track_title|
# +------------------+-------------------+--------------------+------------------+--------------+--------------------+
# |SOUMNSI12AB0182807|Digital Underground|    The Way We Swing|TRMMGKQ128F9325E10|      Linkwood|Whats up with the...|
# |SOCMRBE12AB018C546|         Jimmy Reed|The Sun Is Shinin...|TRMMREB12903CEB1B1|    Slim Harpo|I Got Love If You...|
# |SOLPHZY12AC468ABA8|      Africa HiTech|            Footstep|TRMMBOC12903CEB46E|Marcus Worgull|Drumstern (BONUS ...|
# |SONGHTM12A8C1374EF|     Death in Vegas|        Anita Berber|TRMMITP128F425D8D0|     Valen Hsu|              Shi Yi|
# |SONGXCA12A8C13E82E| Grupo Exterminador|       El Triunfador|TRMMAYZ128F429ECE6|     I Ribelli|           Lei M'Ama|
# |SOMBCRC12A67ADA435|      Fading Friend|         Get us out!|TRMMNVU128EF343EED|     Masterboy|  Feel The Heat 2000|
# |SOTDWDK12A8C13617B|       Daevid Allen|          Past Lives|TRMMNCZ128F426FF0E| Bhimsen Joshi|Raga - Shuddha Sa...|
# |SOEBURP12AB018C2FB|  Cristian Paduraru|          Born Again|TRMMPBS12903CE90E1|     Yespiring|      Journey Stages|
# |SOSRJHS12A6D4FDAA3|         Jeff Mills|  Basic Human Design|TRMWMEL128F421DA68|           M&T|       Drumsettester|
# |SOIYAAQ12A6D4F954A|           Excepter|                  OG|TRMWHRI128F147EA8E|    The Fevers|Não Tenho Nada (N...|
# +------------------+-------------------+--------------------+------------------+--------------+--------------------+

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
triplets.cache()
triplets.show(10, 50)

# +----------------------------------------+------------------+-----+
# |                                 user_id|           song_id|plays|
# +----------------------------------------+------------------+-----+
# |f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|SOQEFDN12AB017C52B|    1|
# |f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|SOQOIUJ12A6701DAA7|    2|
# |f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|SOQOKKD12A6701F92E|    4|
# |f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|SOSDVHO12AB01882C7|    1|
# |f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|SOSKICX12A6701F932|    1|
# |f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|SOSNUPV12A8C13939B|    1|
# |f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|SOSVMII12A6701F92D|    1|
# |f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|SOTUNHI12B0B80AFE2|    1|
# |f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|SOTXLTZ12AB017C535|    1|
# |f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|SOTZDDX12A6701F935|    1|
# +----------------------------------------+------------------+-----+

# left anti join to remove manualy corrected matches from mismatches dataset

mismatches_not_accepted = mismatches.join(matches_manually_accepted, on="song_id", how="left_anti")

# left anti join to remove mismatches from triplets dataset

triplets_not_mismatched = triplets.join(mismatches_not_accepted, on="song_id", how="left_anti")

# print left over mismatches row counts after we removed those manually accepted matched songs

triplets_not_mismatched = triplets_not_mismatched.repartition(partitions).cache()

print(mismatches_not_accepted.count()) 

# 19093 

# print original and mismatch removed row counts

print(triplets.count())                 
print(triplets_not_mismatched.count())  

# 48373586
# 45795111

# Print unique songs and unique users in the dataset

print(f"There are {triplets_not_mismatched.dropDuplicates(['song_id']).count()} unique songs in this dataset.")
print(f"There are {triplets_not_mismatched.dropDuplicates(['user_id']).count()} unique users in this dataset.")

# There are 378310 unique songs in this dataset.
# There are 1019318 unique users in this dataset.


# -----------------------------------------------------------------------------
# Song Recommendations Q1 (b)
# -----------------------------------------------------------------------------

# Different songs most active users playered

mostActive_user = (
    triplets_not_mismatched
    .groupBy("user_id")
    .agg(
        F.sum(F.col('plays')).alias('play_counts'),
        F.countDistinct(F.col('song_id')).alias('uniqueSongCount')
    )
    .orderBy(F.col('play_counts').desc())
)

# Top 10 active users

mostActive_user.show(10,50)

# +----------------------------------------+-----------+---------------+
# |                                 user_id|play_counts|uniqueSongCount|
# +----------------------------------------+-----------+---------------+
# |093cb74eb3c517c5179ae24caf0ebec51b24d2a2|      13074|            195|
# |119b7c88d58d0c6eb051365c103da5caf817bea6|       9104|           1362|
# |3fa44653315697f42410a30cb766a4eb102080bb|       8025|            146|
# |a2679496cd0af9779a92a13ff7c6af5c81ea8c7b|       6506|            518|
# |d7d2d888ae04d16e994d6964214a1de81392ee04|       6190|           1257|
# |4ae01afa8f2430ea0704d502bc7b57fb52164882|       6153|            453|
# |b7c24f770be6b802805ac0e2106624a517643c17|       5827|           1364|
# |113255a012b2affeab62607563d03fbdf31b08e7|       5471|           1096|
# |99ac3d883681e21ea68071019dba828ce76fe94d|       5385|            939|
# |6d625c6557df84b60d90426c0116138b617b9449|       5362|           1307|
# +----------------------------------------+-----------+---------------+

# Different songs top active user playered

uniqueSong_mostActive_user = mostActive_user.select('uniqueSongCount') \
    .rdd.map(lambda row: row[0]) \
    .take(1)[0]

# Print the result and the percentage of total unique songs

print(f"There are {uniqueSong_mostActive_user} different songs played by the most active user.")
print(f"Its percentage of unique song is: {uniqueSong_mostActive_user / uniqueSongCount * 100}%.")

# There are 195 different songs played by the most active user.
# Its percentage of unique song is: 0.05154502920884989%.

# -----------------------------------------------------------------------------
# Song Recommendations Q1 (c)
# -----------------------------------------------------------------------------

# Import packages to plot

import pandas as pd
import matplotlib.pyplot as plt

# Define bins for the histogram plot

buckets = [i for i in range(0, 1100, 50)]

# Plot histogram for the distribution of user activity

play_counts_hist = mostActive_user.select('play_counts').rdd.flatMap(lambda x:x).histogram(buckets)

user_activity_plot = pd.DataFrame(
    list(zip(*play_counts_hist)),
    columns=['bin', 'frequency']
).set_index(
    'bin'
).plot(kind='bar', figsize=(13, 9), color='#86bf91', zorder=2, width=0.85)
user_activity_plot.set_xlabel("Number of Songs per User Played", labelpad=20, weight='bold', size=14)
user_activity_plot.set_ylabel("Number of User", labelpad=20, weight='bold', size=14)
user_activity_plot.set_title("Distribution of User Activity", pad=20, weight='bold', size=16)

plt.savefig('userDistrbution.png')

# List most popular songs played by the users

song_popularity = (
    triplets_not_mismatched
    .groupBy("song_id")
    .agg(
        F.sum(F.col('plays')).alias('play_counts')
    )
    .orderBy(F.col('play_counts').desc())
)

song_popularity.show(10,50)

# +------------------+-----------+
# |           song_id|play_counts|
# +------------------+-----------+
# |SOBONKR12A58A7A7E0|     726885|
# |SOSXLTC12AF72A7F54|     527893|
# |SOEGIYH12A6D4FC0E3|     389880|
# |SOAXGDH12A8C13F8A1|     356533|
# |SONYKOW12AB01849C9|     292642|
# |SOPUCYA12A8C13A694|     274627|
# |SOUFTBI12AB0183F65|     268353|
# |SOVDSJC12A58A7A271|     244730|
# |SOOFYTN12A6D4F9B35|     241669|
# |SOHTKMO12AB01843B0|     236494|
# +------------------+-----------+

# Define bins for the histogram plot

buckets = [i for i in range(0, 1100, 50)]

# Plot histogram for the distribution of song popularity

song_counts_hist = song_popularity.select('play_counts').rdd.flatMap(lambda x:x).histogram(buckets)

song_popularity_plot = pd.DataFrame(
    list(zip(*song_counts_hist)),
    columns=['bin', 'frequency']
).set_index(
    'bin'
).plot(kind='bar', figsize=(13, 9), color='#86bf91', zorder=2, width=0.85)
song_popularity_plot.set_xlabel("Played Times", labelpad=20, weight='bold', size=14)
song_popularity_plot.set_ylabel("Number of Songs", labelpad=20, weight='bold', size=14)
song_popularity_plot.set_title("Distribution of Song Popularity", pad=20, weight='bold', size=16)

plt.savefig('songDistrbution.png')


# -----------------------------------------------------------------------------
# Data analysis
# -----------------------------------------------------------------------------

# Helper

def get_user_counts(triplets):
    return (
        triplets
        .groupBy("user_id")
        .agg(
            F.count(col("song_id")).alias("song_count"),
            F.sum(col("plays")).alias("play_count"),
        )
        .orderBy(col("play_count").desc())
    )

def get_song_counts(triplets):
    return (
        triplets
        .groupBy("song_id")
        .agg(
            F.count(col("user_id")).alias("user_count"),
            F.sum(col("plays")).alias("play_count"),
        )
        .orderBy(col("play_count").desc())
    )

# Different songs most active users playered

user_counts = (
    triplets_not_mismatched
    .groupBy("user_id")
    .agg(
        F.count(col("song_id")).alias("song_count"),
        F.sum(col("plays")).alias("play_count"),
    )
    .orderBy(col("play_count").desc())
)
user_counts.cache()
user_counts.count()

# 1019318

user_counts.show(10, False)

# +----------------------------------------+----------+----------+
# |user_id                                 |song_count|play_count|
# +----------------------------------------+----------+----------+
# |093cb74eb3c517c5179ae24caf0ebec51b24d2a2|195       |13074     |
# |119b7c88d58d0c6eb051365c103da5caf817bea6|1362      |9104      |
# |3fa44653315697f42410a30cb766a4eb102080bb|146       |8025      |
# |a2679496cd0af9779a92a13ff7c6af5c81ea8c7b|518       |6506      |
# |d7d2d888ae04d16e994d6964214a1de81392ee04|1257      |6190      |
# |4ae01afa8f2430ea0704d502bc7b57fb52164882|453       |6153      |
# |b7c24f770be6b802805ac0e2106624a517643c17|1364      |5827      |
# |113255a012b2affeab62607563d03fbdf31b08e7|1096      |5471      |
# |99ac3d883681e21ea68071019dba828ce76fe94d|939       |5385      |
# |6d625c6557df84b60d90426c0116138b617b9449|1307      |5362      |
# +----------------------------------------+----------+----------+

# User statistics

statistics = (
    user_counts
    .select("song_count", "play_count")
    .describe()
    .toPandas()
    .set_index("summary")
    .rename_axis(None)
    .T
)
print(statistics)

#               count                mean              stddev min    max
# song_count  1019318   44.92720721109605   54.91113199747355   3   4316
# play_count  1019318  128.82423149596102  175.43956510304616   3  13074

print(user_counts.approxQuantile("song_count", [0.0, 0.25, 0.5, 0.75, 1.0], 0.05))
print(user_counts.approxQuantile("play_count", [0.0, 0.25, 0.5, 0.75, 1.0], 0.05))

# [3.0, 18.0, 31.0, 73.0, 4316.0]
# [3.0, 36.0, 68.0, 168.0, 13074.0]

# Most popular songs played by the users

song_counts = (
    triplets_not_mismatched
    .groupBy("song_id")
    .agg(
        F.count(col("user_id")).alias("user_count"),
        F.sum(col("plays")).alias("play_count"),
    )
    .orderBy(col("play_count").desc())
)
song_counts.cache()
song_counts.count()

# 378310

song_counts.show(10, False)

# +------------------+----------+----------+
# |song_id           |user_count|play_count|
# +------------------+----------+----------+
# |SOBONKR12A58A7A7E0|84000     |726885    |
# |SOSXLTC12AF72A7F54|80656     |527893    |
# |SOEGIYH12A6D4FC0E3|69487     |389880    |
# |SOAXGDH12A8C13F8A1|90444     |356533    |
# |SONYKOW12AB01849C9|78353     |292642    |
# |SOPUCYA12A8C13A694|46078     |274627    |
# |SOUFTBI12AB0183F65|37642     |268353    |
# |SOVDSJC12A58A7A271|36976     |244730    |
# |SOOFYTN12A6D4F9B35|40403     |241669    |
# |SOHTKMO12AB01843B0|46077     |236494    |
# +------------------+----------+----------+

# Song statistics

statistics = (
    song_counts
    .select("user_count", "play_count")
    .describe()
    .toPandas()
    .set_index("summary")
    .rename_axis(None)
    .T
)
print(statistics)

             # count                mean             stddev min     max
# user_count  378310  121.05181200602681  748.6489783736955   1   90444
# play_count  378310   347.1038513388491  2978.605348838224   1  726885

print(song_counts.approxQuantile("user_count", [0.0, 0.25, 0.5, 0.75, 1.0], 0.05))
print(song_counts.approxQuantile("play_count", [0.0, 0.25, 0.5, 0.75, 1.0], 0.05))

# [1.0, 5.0, 14.0, 58.0, 90444.0]
# [1.0, 9.0, 27.0, 107.0, 726885.0]

# -----------------------------------------------------------------------------
# Song Recommendations Q1 (c) Limiting
# -----------------------------------------------------------------------------

# Thresholds

user_song_count_threshold = 49
song_user_count_threshold = 24

triplets_limited = triplets_not_mismatched

# Remove users who have listened to fewer than 50 songs

triplets_limited = (
    triplets_limited
    .join(
        triplets_limited.groupBy("user_id").count().where(col("count") > user_song_count_threshold).select("user_id"),
        on="user_id",
        how="inner"
    )
)

print(triplets_limited.count())

# 29336544

print(triplets_limited.dropDuplicates(['user_id']).count())
print(triplets_limited.dropDuplicates(['song_id']).count())

# 272287
# 364669

# Remove songs which have been played less than 25 times

triplets_limited = (
    triplets_limited
    .join(
        triplets_limited.groupBy("song_id").count().where(col("count") > song_user_count_threshold).select("song_id"),
        on="song_id",
        how="inner"
    )
)

triplets_limited.cache()
triplets_limited.count() 

# 27563234

# Count how many users left and how many different songs left and their ratio to original dataset

(
    triplets_limited
    .agg(
        countDistinct(col("user_id")).alias('user_count'),
        countDistinct(col("song_id")).alias('song_count')
    )
    .toPandas()
    .T
    .rename(columns={0: "value"})
)

#              value
# user_count  272287 / 1019318 = 0.267126647425043
# song_count  112994 /  378310 = 0.29868097591921966

print(get_user_counts(triplets_limited).approxQuantile("song_count", [0.0, 0.25, 0.5, 0.75, 1.0], 0.05))
print(get_song_counts(triplets_limited).approxQuantile("user_count", [0.0, 0.25, 0.5, 0.75, 1.0], 0.05))

# [1.0, 63.0, 89.0, 120.0, 3056.0]
# [25.0, 42.0, 90.0, 249.0, 41350.0]

# -----------------------------------------------------------------------------
# Encoding
# -----------------------------------------------------------------------------

# Imports

from pyspark.ml.feature import StringIndexer

# Encoding

user_id_indexer = StringIndexer(inputCol="user_id", outputCol="user_id_encoded")
song_id_indexer = StringIndexer(inputCol="song_id", outputCol="song_id_encoded")

user_id_indexer_model = user_id_indexer.fit(triplets_limited)
song_id_indexer_model = song_id_indexer.fit(triplets_limited)

triplets_limited = user_id_indexer_model.transform(triplets_limited)
triplets_limited = song_id_indexer_model.transform(triplets_limited)


# -----------------------------------------------------------------------------
# Splitting
# -----------------------------------------------------------------------------

# Imports

from pyspark.sql.window import *

# Splits

training, test = triplets_limited.randomSplit([0.75, 0.25])

test_not_training = test.join(training, on="user_id", how="left_anti")

training.cache()
test.cache()
test_not_training.cache()

print(f"training:          {training.count()}")
print(f"test:              {test.count()}")
print(f"test_not_training: {test_not_training.count()}")

# training:          20675406
# test:              6887828
# test_not_training: 1

test_not_training.show(50, False)

# +----------------------------------------+------------------+-----+---------------+---------------+
# |user_id                                 |song_id           |plays|user_id_encoded|song_id_encoded|
# +----------------------------------------+------------------+-----+---------------+---------------+
# |78a06d6e1255457c75a5a1b2a94e421f1cff1e64|SOGIVVR12AB0183A54|3    |272286.0       |86676.0        |
# +----------------------------------------+------------------+-----+---------------+---------------+

counts = test_not_training.groupBy("user_id").count().toPandas().set_index("user_id")["count"].to_dict()

temp = (
    test_not_training
    .withColumn("id", monotonically_increasing_id())
    .withColumn("random", rand())
    .withColumn(
        "row",
        row_number()
        .over(
            Window
            .partitionBy("user_id")
            .orderBy("random")
        )
    )
)

for k, v in counts.items():
    temp = temp.where((col("user_id") != k) | (col("row") < v * 0.75))

temp = temp.drop("id", "random", "row")
temp.cache()

temp.show(50, False)

# +-------+-------+-----+---------------+---------------+
# |user_id|song_id|plays|user_id_encoded|song_id_encoded|
# +-------+-------+-----+---------------+---------------+
# +-------+-------+-----+---------------+---------------+

test = test.join(test_not_training, on=["user_id", "song_id"], how="left_anti")
test_not_training = test.join(training, on="user_id", how="left_anti")

print(f"training:          {training.count()}")
print(f"test:              {test.count()}")
print(f"test_not_training: {test_not_training.count()}")

# training:          20675406
# test:              6887827
# test_not_training: 0

# -----------------------------------------------------------------------------
# Modeling
# -----------------------------------------------------------------------------

# Imports

from pyspark.ml.recommendation import ALS

from pyspark.mllib.evaluation import RankingMetrics

# Modeling

als = ALS(maxIter=5, regParam=0.01, userCol="user_id_encoded", itemCol="song_id_encoded", ratingCol="plays", implicitPrefs=True)
als_model = als.fit(training)
predictions = als_model.transform(test)

predictions = predictions.orderBy(col("user_id"), col("song_id"), col("prediction").desc())
predictions.cache()

predictions.show(50, False)

# +----------------------------------------+------------------+-----+---------------+---------------+------------+
# |user_id                                 |song_id           |plays|user_id_encoded|song_id_encoded|prediction  |
# +----------------------------------------+------------------+-----+---------------+---------------+------------+
# |00007ed2509128dcdd74ea3aac2363e24e9dc06b|SOBWGGV12A6D4FD72E|1    |225036.0       |21508.0        |2.8492135E-4|
# |00007ed2509128dcdd74ea3aac2363e24e9dc06b|SOCXHEU12A6D4FB331|1    |225036.0       |61972.0        |6.811644E-5 |
# |00007ed2509128dcdd74ea3aac2363e24e9dc06b|SOGRNDU12A3F1EB51F|2    |225036.0       |96798.0        |6.21283E-5  |
# |00007ed2509128dcdd74ea3aac2363e24e9dc06b|SOKUCDV12AB0185C45|4    |225036.0       |28324.0        |5.338544E-4 |
# |00007ed2509128dcdd74ea3aac2363e24e9dc06b|SOMUZHL12A8C130AFE|1    |225036.0       |23859.0        |2.098176E-4 |
# |00007ed2509128dcdd74ea3aac2363e24e9dc06b|SOOKJWB12A6D4FD4F8|1    |225036.0       |25133.0        |3.6173267E-4|
# |00007ed2509128dcdd74ea3aac2363e24e9dc06b|SOPFVLV12AB0185C5D|2    |225036.0       |48069.0        |1.5972267E-4|
# |00007ed2509128dcdd74ea3aac2363e24e9dc06b|SOVMWUC12A8C13750B|1    |225036.0       |825.0          |0.011364464 |
# |0000bb531aaa657c932988bc2f7fd7fc1b2050ec|SOBHUPL12A670203C9|1    |73366.0        |62209.0        |0.0013660499|
# |0000bb531aaa657c932988bc2f7fd7fc1b2050ec|SOCDKAZ12A8C13C80F|1    |73366.0        |11583.0        |0.0053063477|
# |0000bb531aaa657c932988bc2f7fd7fc1b2050ec|SOCDXHL12A8C137A8A|1    |73366.0        |1501.0         |0.011964262 |
# |0000bb531aaa657c932988bc2f7fd7fc1b2050ec|SODVYCM12A6D4F7F2A|2    |73366.0        |43458.0        |8.179023E-4 |
# |0000bb531aaa657c932988bc2f7fd7fc1b2050ec|SOGDTQS12A6310D7D1|1    |73366.0        |4412.0         |0.011431758 |
# |0000bb531aaa657c932988bc2f7fd7fc1b2050ec|SOIXWDT12A6D4F842F|1    |73366.0        |60090.0        |8.139299E-4 |
# |0000bb531aaa657c932988bc2f7fd7fc1b2050ec|SOJHVCP12A6701D0F3|1    |73366.0        |86309.0        |3.181601E-4 |
# |0000bb531aaa657c932988bc2f7fd7fc1b2050ec|SOJOEIK12A6D4FBE16|1    |73366.0        |46506.0        |3.8808704E-4|
# |0000bb531aaa657c932988bc2f7fd7fc1b2050ec|SOJSKXK12A8C13189C|1    |73366.0        |53079.0        |2.535636E-4 |
# |0000bb531aaa657c932988bc2f7fd7fc1b2050ec|SOKBHQN12A67ADBAE2|2    |73366.0        |12343.0        |0.010346504 |
# |0000bb531aaa657c932988bc2f7fd7fc1b2050ec|SOKJATR12A6D4FA1D3|1    |73366.0        |91546.0        |3.7970184E-4|
# |0000bb531aaa657c932988bc2f7fd7fc1b2050ec|SOKUWEV12A8C13BBEB|2    |73366.0        |1424.0         |0.019350993 |
# |0000bb531aaa657c932988bc2f7fd7fc1b2050ec|SOLKKUO12A679D7E3A|1    |73366.0        |99264.0        |1.5693594E-4|
# |0000bb531aaa657c932988bc2f7fd7fc1b2050ec|SOMCYNS12A58A7B8BE|1    |73366.0        |55099.0        |6.291066E-4 |
# |0000bb531aaa657c932988bc2f7fd7fc1b2050ec|SOMFLEZ12A6D4F907C|1    |73366.0        |12864.0        |0.0072555924|
# |0000bb531aaa657c932988bc2f7fd7fc1b2050ec|SOMMPQA12A67ADA912|2    |73366.0        |88607.0        |4.3872203E-4|
# |0000bb531aaa657c932988bc2f7fd7fc1b2050ec|SOPOXOC12A58A78567|2    |73366.0        |2068.0         |0.018669851 |
# |0000bb531aaa657c932988bc2f7fd7fc1b2050ec|SOQPYQS12A58A7B8DF|2    |73366.0        |8022.0         |0.009667078 |
# |0000bb531aaa657c932988bc2f7fd7fc1b2050ec|SOSOPQD12A6D4F8D32|4    |73366.0        |63638.0        |6.9183466E-4|
# |0000bb531aaa657c932988bc2f7fd7fc1b2050ec|SOSWUDF12A6D4FA15C|1    |73366.0        |100690.0       |2.116717E-4 |
# |0000bb531aaa657c932988bc2f7fd7fc1b2050ec|SOTLDCX12AAF3B1356|1    |73366.0        |2189.0         |0.034425005 |
# |0000bb531aaa657c932988bc2f7fd7fc1b2050ec|SOTTLBW12A6D4F64AF|1    |73366.0        |20005.0        |0.0023382786|
# |0000bb531aaa657c932988bc2f7fd7fc1b2050ec|SOUHBAC12AB01816FE|2    |73366.0        |4834.0         |0.011556094 |
# |0000bb531aaa657c932988bc2f7fd7fc1b2050ec|SOVEUVC12A6310EAF1|1    |73366.0        |646.0          |0.024096524 |
# |0000bb531aaa657c932988bc2f7fd7fc1b2050ec|SOVKQCW12A67ADE9FF|4    |73366.0        |6582.0         |0.013731118 |
# |0000bb531aaa657c932988bc2f7fd7fc1b2050ec|SOXBPBX12AB0183CD1|1    |73366.0        |2671.0         |0.019922948 |
# |0000bb531aaa657c932988bc2f7fd7fc1b2050ec|SOXSQCC12A8C13D78E|1    |73366.0        |4208.0         |0.0055245575|
# |0000bb531aaa657c932988bc2f7fd7fc1b2050ec|SOYRVSP12A6D4F907A|1    |73366.0        |2922.0         |0.012251757 |
# |0000bb531aaa657c932988bc2f7fd7fc1b2050ec|SOZPTMF12AB017F621|2    |73366.0        |110771.0       |3.664778E-5 |
# |0000d3c803e068cf1da17724f1674897b2dd7130|SOCAZTR12A6D4F947F|1    |186787.0       |29923.0        |0.0013398913|
# |0000d3c803e068cf1da17724f1674897b2dd7130|SOHLUAY12A670212FA|1    |186787.0       |27175.0        |0.0048265792|
# |0000d3c803e068cf1da17724f1674897b2dd7130|SOJAMVI12AB017B880|1    |186787.0       |4560.0         |0.012791153 |
# |0000d3c803e068cf1da17724f1674897b2dd7130|SOMDOSP12A6D4F86AA|1    |186787.0       |53709.0        |9.445088E-5 |
# |0000d3c803e068cf1da17724f1674897b2dd7130|SONELXJ12A8C14534E|2    |186787.0       |99290.0        |1.2075724E-4|
# |0000d3c803e068cf1da17724f1674897b2dd7130|SONSEAJ12A6D4F96A7|13   |186787.0       |58392.0        |0.0016257903|
# |0000d3c803e068cf1da17724f1674897b2dd7130|SONTWIG12A6D4F95B6|1    |186787.0       |10073.0        |0.0038263586|
# |0000d3c803e068cf1da17724f1674897b2dd7130|SOOYAYM12A8C13FE7E|2    |186787.0       |5191.0         |0.0069947992|
# |0000d3c803e068cf1da17724f1674897b2dd7130|SOQMXGC12A8C1346B7|1    |186787.0       |10444.0        |0.0042368826|
# |0000d3c803e068cf1da17724f1674897b2dd7130|SOSGFXU12A6D4F61CA|3    |186787.0       |95973.0        |5.944257E-4 |
# |0000d3c803e068cf1da17724f1674897b2dd7130|SOSPESY12A6D4FCEC2|1    |186787.0       |67498.0        |2.0196781E-5|
# |0000d3c803e068cf1da17724f1674897b2dd7130|SOUKZLZ12A6D4F869D|6    |186787.0       |53386.0        |1.5812027E-4|
# |0000d3c803e068cf1da17724f1674897b2dd7130|SOURVJI12A58A7F353|2    |186787.0       |1637.0         |0.02164652  |
# +----------------------------------------+------------------+-----+---------------+---------------+------------+


# -----------------------------------------------------------------------------
# Metrics
# -----------------------------------------------------------------------------

# Helpers 

def extract_songs_top_k(x, k):
    x = sorted(x, key=lambda x: -x[1])
    return [x[0] for x in x][0:k]

extract_songs_top_k_udf = udf(lambda x: extract_songs_top_k(x, k), ArrayType(IntegerType()))

def extract_songs(x):
    x = sorted(x, key=lambda x: -x[1])
    return [x[0] for x in x]

extract_songs_udf = udf(lambda x: extract_songs(x), ArrayType(IntegerType()))

# Recommendations

k = 5

topK = als_model.recommendForAllUsers(k)

topK.cache()
topK.count() 

# 272286

topK.show(10, False)

# +---------------+-------------------------------------------------------------------------------------------+
# |user_id_encoded|recommendations                                                                            |
# +---------------+-------------------------------------------------------------------------------------------+
# |12             |[[2, 1.7223196], [9, 1.4665143], [12, 1.4645863], [7, 1.3623036], [5, 1.3493447]]          |
# |18             |[[12, 0.91743344], [9, 0.84102124], [7, 0.80987585], [13, 0.7236692], [149, 0.713349]]     |
# |38             |[[65, 0.572907], [2314, 0.524168], [265, 0.50228274], [320, 0.48926678], [292, 0.48180884]]|
# |67             |[[6, 1.1974878], [2, 1.1549268], [0, 1.1234317], [14, 1.0665233], [30, 0.9776364]]         |
# |70             |[[0, 1.1838703], [11, 1.0766106], [7, 1.0727123], [12, 1.0520372], [9, 0.9949671]]         |
# |93             |[[42, 1.2146089], [197, 0.9530904], [28, 0.9036891], [85, 0.8695617], [0, 0.8663203]]      |
# |161            |[[6, 0.7765956], [0, 0.7537621], [100, 0.7251642], [38, 0.7089893], [2, 0.68993336]]       |
# |186            |[[13, 0.5306508], [12, 0.45506445], [149, 0.42721397], [66, 0.4123716], [9, 0.40673938]]   |
# |190            |[[0, 0.6713956], [42, 0.6021439], [85, 0.53880185], [38, 0.5238066], [11, 0.4727557]]      |
# |218            |[[0, 0.7791968], [13, 0.6681122], [62, 0.60353553], [42, 0.5745869], [85, 0.54676396]]     |
# +---------------+-------------------------------------------------------------------------------------------+


recommended_songs = (
    topK
    .withColumn("recommended_songs", extract_songs_top_k_udf(col("recommendations")))
    .select("user_id_encoded", "recommended_songs")
)
recommended_songs.cache()
recommended_songs.count() 

# 272286

recommended_songs.show(10, 50)

# +---------------+-------------------------+
# |user_id_encoded|        recommended_songs|
# +---------------+-------------------------+
# |             12|         [2, 9, 12, 7, 5]|
# |             18|      [12, 9, 7, 13, 149]|
# |             38|[65, 2314, 265, 320, 292]|
# |             67|        [6, 2, 0, 14, 30]|
# |             70|        [0, 11, 7, 12, 9]|
# |             93|     [42, 197, 28, 85, 0]|
# |            161|       [6, 0, 100, 38, 2]|
# |            186|     [13, 12, 149, 66, 9]|
# |            190|      [0, 42, 85, 38, 11]|
# |            218|      [0, 13, 62, 42, 85]|
# +---------------+-------------------------+


# Relevant songs

relevant_songs = (
    test
    .select(
        col("user_id_encoded").cast(IntegerType()),
        col("song_id_encoded").cast(IntegerType()),
        col("plays").cast(IntegerType())
    )
    .groupBy('user_id_encoded')
    .agg(
        collect_list(
            array(
                col("song_id_encoded"),
                col("plays")
            )
        ).alias('relevance')
    )
    .withColumn("relevant_songs", extract_songs_udf(col("relevance")))
    .select("user_id_encoded", "relevant_songs")
)
relevant_songs.cache()
relevant_songs.count()

# 272286

relevant_songs.show(10, 50)

# +---------------+--------------------------------------------------+
# |user_id_encoded|                                    relevant_songs|
# +---------------+--------------------------------------------------+
# |             12|[1140, 532, 448, 219, 49, 263, 204, 538, 574, 4...|
# |             18|[22976, 19131, 20934, 9269, 3128, 21154, 9, 887...|
# |             38|[10721, 45137, 18340, 15124, 18475, 17243, 1915...|
# |             67|[34626, 38737, 116, 7727, 22383, 22, 2076, 3648...|
# |             70|[68641, 2032, 6745, 1911, 15046, 87409, 4634, 1...|
# |             93|[45280, 3637, 7227, 504, 7082, 20611, 36963, 13...|
# |            161|[41911, 3971, 228, 23030, 2776, 40825, 11450, 3...|
# |            186|[244, 13778, 9402, 3934, 164, 541, 45924, 10961...|
# |            190|[88370, 1384, 85, 11338, 2649, 3595, 105421, 37...|
# |            218|[781, 39727, 959, 53770, 102139, 13500, 2485, 3...|
# +---------------+--------------------------------------------------+

combined = (
    recommended_songs.join(relevant_songs, on='user_id_encoded', how='inner')
    .rdd
    .map(lambda row: (row[1], row[2]))
)
combined.cache()
combined.count()

# 272286

combined.take(3)

# ([2, 9, 12, 7, 5], [1140, 532, 448, 219, 49, 263, 204, 538, 574, 495, 1891, 731 ...])
# ([149, 12, 11, 13, 7], [13867, 15662, 1567, 3128, 9, 377, 15561, 26608, 17597, 62987 ...])
# ([65, 2314, 320, 1486, 2002], [9818, 16700, 19482, 9750, 45200, 53543, 24419, 7985 ...])

rankingMetrics = RankingMetrics(combined)
precisionAtK = rankingMetrics.precisionAt(k)
ndcgAtK = rankingMetrics.ndcgAt(k)
meanAveragePrecision = rankingMetrics.meanAveragePrecision


print(f"Precision @ 5:                {precisionAtK}")
print(f"NDCG @ 5:                     {ndcgAtK}")
print(f"Mean Average Precision (MAP): {meanAveragePrecision}")

# Precision @ 5:                0.05441117060737613
# NDCG @ 5:                     0.05642662880352322
# Mean Average Precision (MAP): 0.006618085491742346

# -----------------------------------------------------------------------------
# Metrics k = 10
# -----------------------------------------------------------------------------

# Recommendations

k = 10

topK = als_model.recommendForAllUsers(k)

topK.cache()
topK.count() 

# 272287

topK.show(10, 150)

# +---------------+------------------------------------------------------------------------------------------------------------------------------------------------------+
# |user_id_encoded|                                                                                                                                       recommendations|
# +---------------+------------------------------------------------------------------------------------------------------------------------------------------------------+
# |             12|[[2, 1.7223196], [9, 1.4665143], [12, 1.4645863], [7, 1.3623036], [5, 1.3493447], [112, 1.3344187], [190, 1.3220387], [3, 1.2732884], [169, 1.26968...|
# |             18|[[12, 0.91743344], [9, 0.84102124], [7, 0.80987585], [13, 0.7236692], [149, 0.713349], [161, 0.66591275], [0, 0.6642568], [308, 0.64975154], [112, ...|
# |             38|[[65, 0.572907], [2314, 0.524168], [265, 0.50228274], [320, 0.48926678], [292, 0.48180884], [158, 0.48092818], [1486, 0.47279832], [2002, 0.467323]...|
# |             67|[[6, 1.1974878], [2, 1.1549268], [0, 1.1234317], [14, 1.0665233], [30, 0.9776364], [38, 0.9383959], [19, 0.92614937], [73, 0.9254446], [3, 0.898961...|
# |             70|[[0, 1.1838703], [11, 1.0766106], [7, 1.0727123], [12, 1.0520372], [9, 0.9949671], [5, 0.92000866], [26, 0.8594886], [23, 0.8578203], [62, 0.827241...|
# |             93|[[42, 1.2146089], [197, 0.9530904], [28, 0.9036891], [85, 0.8695617], [0, 0.8663203], [13, 0.86184996], [124, 0.85385454], [59, 0.8504684], [318, 0...|
# |            161|[[6, 0.7765956], [0, 0.7537621], [100, 0.7251642], [38, 0.7089893], [2, 0.68993336], [14, 0.64519507], [85, 0.6342677], [30, 0.61492556], [65, 0.59...|
# |            186|[[13, 0.5306508], [12, 0.45506445], [149, 0.42721397], [66, 0.4123716], [9, 0.40673938], [37, 0.4051898], [61, 0.39819506], [23, 0.39620805], [36, ...|
# |            190|[[0, 0.6713956], [42, 0.6021439], [85, 0.53880185], [38, 0.5238066], [11, 0.4727557], [197, 0.45187628], [2, 0.43854684], [30, 0.43167514], [124, 0...|
# |            218|[[0, 0.7791968], [13, 0.6681122], [62, 0.60353553], [42, 0.5745869], [85, 0.54676396], [28, 0.5342463], [23, 0.5321162], [40, 0.52380407], [66, 0.5...|
# +---------------+------------------------------------------------------------------------------------------------------------------------------------------------------+


recommended_songs = (
    topK
    .withColumn("recommended_songs", extract_songs_top_k_udf(col("recommendations")))
    .select("user_id_encoded", "recommended_songs")
)
recommended_songs.cache()
recommended_songs.count() 

# 272286

recommended_songs.show(10, 50)

# +---------------+-------------------------+
# |user_id_encoded|        recommended_songs|
# +---------------+-------------------------+
# |             12|         [2, 9, 12, 7, 5]|
# |             18|      [12, 9, 7, 13, 149]|
# |             38|[65, 2314, 265, 320, 292]|
# |             67|        [6, 2, 0, 14, 30]|
# |             70|        [0, 11, 7, 12, 9]|
# |             93|     [42, 197, 28, 85, 0]|
# |            161|       [6, 0, 100, 38, 2]|
# |            186|     [13, 12, 149, 66, 9]|
# |            190|      [0, 42, 85, 38, 11]|
# |            218|      [0, 13, 62, 42, 85]|
# +---------------+-------------------------+

# Relevant songs

relevant_songs = (
    test
    .select(
        col("user_id_encoded").cast(IntegerType()),
        col("song_id_encoded").cast(IntegerType()),
        col("plays").cast(IntegerType())
    )
    .groupBy('user_id_encoded')
    .agg(
        collect_list(
            array(
                col("song_id_encoded"),
                col("plays")
            )
        ).alias('relevance')
    )
    .withColumn("relevant_songs", extract_songs_udf(col("relevance")))
    .select("user_id_encoded", "relevant_songs")
)
relevant_songs.cache()
relevant_songs.count()

# 272286

relevant_songs.show(10, 50)

# +---------------+--------------------------------------------------+
# |user_id_encoded|                                    relevant_songs|
# +---------------+--------------------------------------------------+
# |             12|[1140, 532, 448, 219, 49, 538, 204, 263, 574, 4...|
# |             18|[22976, 20934, 19131, 9269, 3128, 21154, 9, 887...|
# |             38|[10721, 45137, 18340, 15124, 17243, 18475, 2008...|
# |             67|[34626, 38737, 116, 7727, 22383, 2076, 22, 104,...|
# |             70|[68641, 2032, 6745, 1911, 4634, 87409, 15046, 1...|
# |             93|[45280, 3637, 7227, 504, 7082, 20611, 36963, 13...|
# |            161|[3971, 41911, 228, 23030, 2776, 11450, 3596, 40...|
# |            186|[13778, 244, 164, 541, 3934, 9402, 7785, 34, 10...|
# |            190|[88370, 1384, 85, 11338, 2649, 25753, 3595, 105...|
# |            218|[781, 39727, 13500, 959, 53770, 102139, 2485, 6...|
# +---------------+--------------------------------------------------+


combined = (
    recommended_songs.join(relevant_songs, on='user_id_encoded', how='inner')
    .rdd
    .map(lambda row: (row[1], row[2]))
)
combined.cache()
combined.count()

# 272286

combined.take(3)

# ([2, 9, 12, 7, 5], [1140, 532, 448, 219, 49, 538, 204, 263, 574, 495, 1891, 747 ...])
# ([149, 12, 11, 13, 7], [13867, 15662, 1567, 3128, 9, 377, 15561, 26608, 17597, 62987 ...])
# ([65, 2314, 320, 1486, 2002], [9818, 16700, 19482, 9750, 45200, 53543, 24419, 7985 ...])


rankingMetrics = RankingMetrics(combined)
precisionAtK = rankingMetrics.precisionAt(k)
ndcgAtK = rankingMetrics.ndcgAt(k)
meanAveragePrecision = rankingMetrics.meanAveragePrecision

print(f"Precision @ 10:               {precisionAtK}")
print(f"NDCG @ 10:                    {ndcgAtK}")
print(f"Mean Average Precision (MAP): {meanAveragePrecision}")

# Precision @ 10:               0.027205585303688065
# NDCG @ 10:                    0.03669888934824079
# Mean Average Precision (MAP): 0.006618085491742346


