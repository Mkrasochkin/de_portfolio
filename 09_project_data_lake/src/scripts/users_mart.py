from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.window import Window
import os
import findspark
from pyspark.sql.functions import col, countDistinct, sum as spark_sum, desc
from pyspark.sql.functions import col, countDistinct, collect_list, row_number, datediff


findspark.init()
findspark.find()
# Настройка окружений Hadoop и Java
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME'] = '/usr'
os.environ['SPARK_HOME'] = '/usr/lib/spark'
os.environ['PYTHONPATH'] = '/usr/local/lib/python3.8'
os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"




def create_user_analytics_mart(spark):
    """
    Создает витрину данных с информацией о городе пользователя для каждого события.
    Алгоритм:
    1. Загружает события и данные о городах  
    2. Для каждого сообщения определяет ближайший город  
    3. Сохраняет результаты в витрину  
    """

    # Загружаем данные
    events = spark.read.parquet("/user/master/data/geo/events")
    cities = spark.read.csv("/user/mkrasochki/data/geo/geo.csv", header=True, inferSchema=True)

    # Фильтруем только сообщения с координатами
    geo_events = events.filter(
        (F.col("event_type") == "message") & (F.col("event.lat").isNotNull()) &(F.col("event.lon").isNotNull())
    )

    # Подготовка данных для расчета расстояния
    city_coords = cities.select(
        F.col("id").alias("city_id"),
        F.col("city"),
        F.radians(F.col("lat")).alias("city_lat_rad"),
        F.radians(F.col("lng")).alias("city_lon_rad")
    )

    # Декартово произведение сообщений и городов
    cross_join = geo_events.crossJoin(city_coords)

    # Расчет расстояний по формуле
    events_with_distances = cross_join.WithColumn(
        "distance",
        2 * 6371 * F.asin(F.sqrt(
            F.pow(F.sin((F.radians(F.col("event.lat")) - F.col("city_lat_rad")) / 2), 2) +
            F.cos(F.radians(F.col("event.lat"))) * 
            F.cos(F.col("city_lat_rad")) *  
            F.pow(F.sin((F.radians(F.col("event.lon")) - F.col("city_lon_rad")) / 2), 2)
        ))
    )

    # Находим ближайший город для каждого соощения
    window = Window.partitionBy("message_id").orderBy("distance")
    nearest_city = events_with_distances.WithColumn("rank", F.row_number().over(window)) \
        .filter("rank == 1") \
        .select(
            "event.message_id",
            "event.message_from",
            "event.datetime",
            "city_id",
            "city",
            "distance"
        )
    
    # Сохраняем результаты
    nearest_city.write.parguet(
        "/user/mkrasochki/data/analytics/user_cities",
        mode="overwrite"
    )

    if __name__ == "__main__":
        spark = SparkSession.builder.appName("UserCityMart").getOrCreate()
        create_user_analytics_mart(spark)
        spark.stop()


def create_user_cities_mart(spark):
    """
    Создает витрину с актуальным и домашним городом пользователей.
    Алгоритм:
    1. Определяет актуальный город (последний известный)
    2. Вычисляет домашний город (пребывание >27 дней)
    3. Сохраняет результаты в витрину  
    """

    # Загружаем ранне созданные данные о городах пользователей
    user_cities = spark.read.parquet("/user/mkrasochki/data/analytics/user_cities")

    # Определяем актуальный город (последнее сообщение)
    window_last = Window.partitionBy("message_from").orderBy(F.desc("last_seen"))
    actual_city = user_cities.withColumn("rn", F.row_number().over(window_last)) \
        .filter("rn = 1") \
        .select(
            "message_from",
            F.col("city").alias("act_city")
        )
    
    # Определяем домашний город (пребывание >27 дней)
    # Критерий: город с максимальной продолжительностью пребывания,
    # где разница между первым и последним сообщением >=27 дней
    user_cities_seq = user_cities.orderBy("message_from", "datetime")
    w = Window.partitionBy("message_from").orderBy("datetime")
    user_cities_seq = user_cities_seq.withColumn("prev_city", F.lag("city").over(w))
    changes = user_cities_seq.filter(col("prev_city") != col("city"))

    # Формируем группы по смене города
    change_group = changes.withColumn("change_group", F.sum(F.when(changes.prev_city.isNotNull(), 1).otherwise(0)).over(w))
    group_stats = change_group.groupBy("message_from", "change_group", "city").agg(
        F.first("datetime").alias("start_date"),
        F.last("datetime").alias("end_date")
    )

    # Находим максимальный интервал пребывания в городе
    home_candidate = group_stats.filter(datediff(group_stats.end_date, group_stats.start_date) >= 27)
    window_home = Window.partitionBy("message_from").orderBy(desc("datediff(end_date, start_date)"))
    home_city = home_candidate.withColumn("rn", row_number().over(window_home)) \
        .filter("rn = 1") \
        .select("message_from", col("city").alias("home_city"))

    # Объединяем результаты в финальную витрину
    user_cities_mart = actual_city.join(
        home_city,
        "message_from",
        "left"
    ).withColumnRenamed("message_from", "user_id")

    # Сохраняем витрину
    user_cities_mart.write.parquet(
        "/user/mkrasochki/data/marts/user_cities_mart",
        mode="overwrite"
    )

if __name__ == "__main__":
    spark = SparkSession.builder.appName("UserCitiesMart").getOrCreate()
    create_user_cities_mart(spark)  
    spark.stop()



def enhance_user_cities_mart(spark):
    """
    Расширяем витрину данными о путешествиях и временем последнего события.
    Добавляем поля:
    - travel_count: количество уникальных посещений городов (с повторами)
    - travel_array: хронологический список посещённых городов
    - local_time: местное время последнего события с учётом временной зоны
    """
    
    # 1. Загружаем ранее созданные данные
    user_events = spark.read.parquet("/user/mkrasochki/data/analytics/user_cities")
    geo_data = spark.read.csv("/user/mkrasochki/data/geo/geo.csv", sep=';', header=True, inferSchema=True)
    
    # 2. Вычисляем хронологию посещения городов
    city_sequence = user_events.select(
        "message_from",
        "city",
        "datetime",
        "first_seen",
        "last_seen"
    ).withColumn(
        "visit_order",
        F.row_number().over(Window.partitionBy("message_from").orderBy("first_seen"))
    )

    
    # Оставляем только смены городов
    w_visit = Window.partitionBy("message_from").orderBy("visit_order")
    transitions = city_sequence.withColumn("prev_city", F.lag("city").over(w_visit)).filter(col("prev_city") != col("city"))

    # Собираем список городов без дубликатов
    travel_history = transitions.groupBy("message_from").agg(
        countDistinct("city").alias("travel_count"),
        collect_list("city").alias("travel_array")
    )

    # Определяем последнее событие и соответствующее ему местное время
    last_event = user_events.join(
        geo_data,
        user_events.city == geo_data.city,
        "left"
    ).withColumn(
        "rn",
        row_number().over(Window.partitionBy("message_from").orderBy(F.desc("last_seen")))
    ).filter("rn = 1").select(
        "message_from",
        "last_seen",
        "timezone",
        "lat",  # добавляем координаты
        "lon"
    )

    # Вычисляем местное время последнего события
    last_event_local = last_event.withColumn(
        "local_time",
        F.from_utc_timestamp(col("last_seen"), col("timezone"))
    ).select(
        "message_from",
        "local_time",
        "lat",  # сохраняем координаты
        "lon"
    )
    
    # 6. Объединяем все данные с основной витриной
    user_cities_mart = spark.read.parquet("/user/mkrasochki/data/marts/user_cities_mart")
    
    enhanced_mart = user_cities_mart.join(
        travel_history,
        user_cities_mart.user_id == travel_history.message_from,
        "left"
    ).join(
        last_event_local,
        user_cities_mart.user_id == last_event_local.message_from,
        "left"
    ).select(
        "user_id",
        "act_city",
        "home_city",
        "travel_count",
        "travel_array",
        "local_time",
        "lat",  # добавляем координаты
        "lon"
    )
    

    # 7. Сохраняем обновлённую витрину
    enhanced_mart.write.parquet(
        "/user/mkrasochki/data/marts/user_cities_mart_enhanced",
        mode="overwrite"
    )

if __name__ == "__main__":
    ‘
    enhance_user_cities_mart(spark)
    spark.stop()

