from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.window import Window
from pyspark.sql.functions import col
import os
import findspark
from pyspark.sql.functions import col, countDistinct, sum as spark_sum, desc


findspark.init()
findspark.find()
# Настройка окружений Hadoop и Java
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME'] = '/usr'
os.environ['SPARK_HOME'] = '/usr/lib/spark'
os.environ['PYTHONPATH'] = '/usr/local/lib/python3.8'
os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"



def create_geo_zone_mart(spark):
    """
    Создает витрину с метриками событий по городам с разбивкой по неделям и месяцам  
    Новых пользователей определяем по первым сообщениям  
    """
    
    # Загрузка данных  
    events = spark.read.parquet("/user/master/data/geo/events")
    cities = spark.read.csv("/user/mkrasochki/data/geo/geo.csv", header=True, inferSchema=True)
    
    # 1. Определяем город для каждого события  
    geo_events = events.filter(
        F.col("lat").isNotNull() & 
        F.col("lon").isNotNull()
    ).select(
        "event_id",
        "event_type",
        "event_time",
        "lat",
        "lon",
        F.when(F.col("event_type") == "message", F.col("event.message_from")).alias("user_id")
    )
    
    # Определяем первые сообщения пользователей  
    first_messages = geo_events.filter(F.col("event_type") == "message") \
        .withColumn("rn", F.row_number().over(Window.partitionBy("user_id").orderBy("event_time"))) \
        .filter("rn = 1") \
        .select("event_id", F.lit(1).alias("is_new_user"))
    
    # Соединение с городами и расчет расстояния  
    cross_join = geo_events.crossJoin(cities)
    events_with_city = cross_join.withColumn(
        "distance",
        2 * 6371 * F.asin(F.sqrt(
            F.pow(F.sin((F.radians(F.col("lat")) - F.radians(F.col("lat_city")))/2), 2) +
            F.cos(F.radians(F.col("lat"))) * F.cos(F.radians(F.col("lat_city"))) *  
            F.pow(F.sin((F.radians(F.col("lon")) - F.radians(F.col("lon_city")))/2), 2)
        ))
    )
    
    # Выбираем ближайший город для каждого события  
    nearest_city = events_with_city.withColumn(
        "rn",
        F.row_number().over(Window.partitionBy("event_id").orderBy("distance"))
    ).filter("rn = 1") \
     .join(first_messages, "event_id", "left") \
     .select(
        "event_id",
        "event_type",
        "event_time",
        F.col("city_id").alias("zone_id"),
        F.date_trunc("month", "event_time").alias("month_begin"),
        F.date_trunc("week", "event_time").alias("week_begin"),
        F.coalesce(F.col("is_new_user"), F.lit(0)).alias("is_new_user")
    )
    
    # 2. Агрегация метрик  
    # Для недельных метрик  
    weekly_metrics = nearest_city.groupBy("zone_id", "week_begin").agg(
        F.count(F.when(F.col("event_type") == "message", 1)).alias("week_message"),
        F.count(F.when(F.col("event_type") == "reaction", 1)).alias("week_reaction"),
        F.count(F.when(F.col("event_type") == "subscription", 1)).alias("week_subscription"),
        F.sum(F.col("is_new_user")).alias("week_user")  # Считаем новых пользователей  
    )
    
    # Для месячных метрик  
    monthly_metrics = nearest_city.groupBy("zone_id", "month_begin").agg(
        F.count(F.when(F.col("event_type") == "message", 1)).alias("month_message"),
        F.count(F.when(F.col("event_type") == "reaction", 1)).alias("month_reaction"),
        F.count(F.when(F.col("event_type") == "subscription", 1)).alias("month_subscription"),
        F.sum(F.col("is_new_user")).alias("month_user")  # Считаем новых пользователей  
    )
    
    # 3. Объединение результатов  
    geo_zone_mart = weekly_metrics.join(
        monthly_metrics,
        ["zone_id"],
        how="full"
    ).select(
        F.col("month_begin").alias("month"),
        F.col("week_begin").alias("week"),
        "zone_id",
        "week_message",
        "week_reaction",
        "week_subscription",
        "week_user",
        "month_message",
        "month_reaction",
        "month_subscription",
        "month_user"
    )
    
    # 4. Сохранение витрины  
    geo_zone_mart.write.parquet(
        "/user/mkrasochki/data/marts/geo_zone_mart",
        mode="overwrite",
        partitionBy=["month"]
    )

if __name__ == "__main__":
    spark = SparkSession.builder.appName("GeoZoneMart").getOrCreate()
    create_geo_zone_mart(spark)
    spark.stop()



