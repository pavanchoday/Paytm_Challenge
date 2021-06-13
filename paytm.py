from pyspark.sql.functions import sum, avg, max, min, mean, count
from pyspark.sql.functions import col, unix_timestamp, to_date
from pyspark.sql.functions import date_format
from datetime import datetime
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DateType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import lag
from pyspark.sql.functions import lead, datediff, when
from pyspark.sql.functions import rank


# -----------------------   Step 1 - Setting Up the Data  -----------------------------

# Reading Data into Spark
weather = spark.read.options(header="true", inferSchema="true").csv("/FileStore/tables/part_*")
country_list = spark.read.options(header="true", inferSchema="true").csv("/FileStore/tables/station/countrylist.csv")
station_list = spark.read.options(header="true", inferSchema="true").csv("/FileStore/tables/station/stationlist.csv")

# Rename STN-- to STN_NO
weather = weather.withColumnRenamed("STN---", "STN_NO")

# Join the stationlist.csv with the countrylist.csv
full_country_names = country_list.join(station_list, ["COUNTRY_ABBR"])

# Join the global weather data with the full country names by station number
weather_station_join = weather.join(full_country_names, ["STN_NO"])


# --------------------  Step 2 Questions  ----------------------------------

# 1. Which country had the hottest average mean temperature over the year?
hottest_temp = (
    weather_station_join.filter(weather_station_join.TEMP != 9999.9)
    .groupBy("COUNTRY_FULL")
    .agg(avg("TEMP").alias("avg_temp"))
    .sort(col("avg_temp").desc())
    .limit(1)
)

hottest_temp.show()


# 2. Which country had the most consecutive days of tornadoes/funnel cloud formations?


def parse_dt(dt):
    dt = str(dt)
    y = dt[:4]
    m = dt[4:6]
    d = dt[6:9]
    return y + "-" + m + "-" + d


convertUDF = udf(lambda z: parse_dt(z))
a = (
    weather_station_join.select("COUNTRY_FULL", "FRSHTT", "YEARMODA")
    .filter(weather_station_join.FRSHTT == "000001")
    .withColumn("YEARMODA", convertUDF(col("YEARMODA")))
    .withColumn("YEARMODA", to_date(col("YEARMODA"), "yyyy-MM-dd"))
)

windowSpec = Window.partitionBy("COUNTRY_FULL").orderBy("YEARMODA")

a = (
    a.withColumn("lead", lead("YEARMODA", 1).over(windowSpec))
    .withColumn("diff", datediff(col("YEARMODA"), col("lead")))
    .withColumn("tag", when(col("diff") >= -1, 1).otherwise(0))
    .sort(col("tag").desc())
    .limit(1)
)

a.show()


# 3. Which country had the second highest average mean wind speed over the year?

wind = (
    weather_station_join.filter(weather_station_join.TEMP != 999.9)
    .groupBy("COUNTRY_FULL")
    .agg(avg("WDSP").alias("avg_WDSP"))
)
wndw = Window.orderBy(col("avg_WDSP").desc())

wind = wind.withColumn("rank", rank().over(wndw))
wind.filter(wind.rank == 2).show()

#Windding
