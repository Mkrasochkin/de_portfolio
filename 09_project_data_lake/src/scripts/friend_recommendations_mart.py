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




def create_friend_recommendations_mart(spark):
    """
    Создает витрину для рекомендаций друзей на основе:
    - общих подписок на каналы (из events)
    - отсутствия истории переписки  
    - нахождения на расстоянии ≤1 км  
    """
    
    # 1. Загрузка всех событий  
    events = spark.read.parquet("/user/master/data/geo/events")
    
    # 2. Извлекаем подписки и сообщения из единой таблицы events  
    subscriptions = events.filter(F.col("event_type") == "subscription").select(
        F.col("event.user").alias("user_id"),
        F.col("event.subscription_channel").alias("channel_id")
    )
    
    messages = events.filter(F.col("event_type") == "message").select(
        F.col("event.message_from").alias("message_from"),
        F.col("event.message_to").alias("message_to")
    )
    
    # 3. Получаем геоданные пользователей (из ранее созданной витрины)
    users_geo = spark.read.parquet("/user/mkrasochki/data/marts/user_cities_mart_enhanced")
    
    # 4. Находим пользователей с общими подписками  
    common_subs = subscriptions.alias("s1").join(
        subscriptions.alias("s2"),
        (F.col("s1.channel_id") == F.col("s2.channel_id")) & 
        (F.col("s1.user_id") < F.col("s2.user_id")),  # Исключаем дубликаты пар  
        "inner"
    ).select(
        F.col("s1.user_id").alias("user_left"),
        F.col("s2.user_id").alias("user_right"),
        F.col("s1.channel_id").alias("common_channel")
    )
    
    # 5. Исключаем пользователей, которые уже переписывались  
    no_messages = common_subs.join(
        messages,
        ((F.col("user_left") == F.col("message_from") & F.col("user_right") == F.col("message_to")) |
         (F.col("user_left") == F.col("message_to") & F.col("user_right") == F.col("message_from"))),
        "left_anti"
    )
    
    # 6. Добавляем геоданные и фильтруем по расстоянию  
    geo_filtered = no_messages.join(
        users_geo.alias("ul"),
        F.col("user_left") == F.col("ul.user_id"),
        "left"
    ).join(
        users_geo.alias("ur"),
        F.col("user_right") == F.col("ur.user_id"),
        "left"
    ).withColumn(
        "distance",
        F.acos(
            F.sin(F.radians(F.col("ul.lat"))) * F.sin(F.radians(F.col("ur.lat"))) +
            F.cos(F.radians(F.col("ul.lat"))) * F.cos(F.radians(F.col("ur.lat"))) *
            F.cos(F.radians(F.col("ul.lon")) - F.radians(F.col("ur.lon")))
        ) * 6371  # Расстояние в км  
    ).filter(
        F.col("distance") <= 1  # Фильтр по расстоянию  
    ).select(
        "user_left",
        "user_right",
        F.current_timestamp().alias("processed_dttm"),  
        F.coalesce(F.col("ul.city_id"), F.lit(0)).alias("zone_id"),
        F.coalesce(F.col("ul.local_time"), F.current_timestamp()).alias("local_time")
    )
    
    # 7. Сохраняем витрину  
    geo_filtered.write.parquet(
        "/user/mkrasochki/data/marts/friend_recommendations",
        mode="overwrite"
    )

if __name__ == "__main__":
    spark = SparkSession.builder.appName("FriendRecommendations").getOrCreate()
    create_friend_recommendations_mart(spark)
    spark.stop()