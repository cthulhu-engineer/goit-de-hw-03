from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, round

spark = SparkSession.builder.appName("Data Processing").getOrCreate()

users_file = "users.csv"
purchases_file = "purchases.csv"
products_file = "products.csv"

# Завантажте та прочитайте кожен CSV-файл як окремий DataFrame
users_df = spark.read.csv(users_file, header=True, inferSchema=True)
purchases_df = spark.read.csv(purchases_file, header=True, inferSchema=True)
products_df = spark.read.csv(products_file, header=True, inferSchema=True)

# Очистіть дані, видаляючи будь-які рядки з пропущеними значеннями
users_df = users_df.dropna()
purchases_df = purchases_df.dropna()
products_df = products_df.dropna()

# Визначте загальну суму покупок за кожною категорією продуктів
purchases_with_products = purchases_df.join(products_df, "product_id")
purchases_with_products = purchases_with_products.withColumn(
    "total_amount", col("quantity") * col("price")
)
full_data = purchases_with_products.join(users_df, "user_id")
total_by_category = full_data.groupBy("category").agg(
    spark_sum("total_amount").alias("total_category_amount")
)

# Визначте суму покупок за кожною категорією продуктів для вікової категорії від 18 до 25 включно
age_filtered_data = full_data.filter((col("age") >= 18) & (col("age") <= 25))
total_by_category_age_18_25 = age_filtered_data.groupBy("category").agg(
    spark_sum("total_amount").alias("total_category_amount_18_25")
)

# Визначте частку покупок за кожною категорією товарів від сумарних витрат для вікової категорії від 18 до 25 років
percentage_by_category_age_18_25 = total_by_category_age_18_25.join(
    total_by_category_age_18_25.select(spark_sum("total_category_amount_18_25").alias("total_all_categories_18_25"))
).withColumn(
    "percentage", round((col("total_category_amount_18_25") / col("total_all_categories_18_25")) * 100, 2)
)

# Виберіть 3 категорії продуктів з найвищим відсотком витрат споживачами віком від 18 до 25 років
top_3_categories = percentage_by_category_age_18_25.orderBy(col("percentage").desc()).limit(3)

# Результати
total_by_category.show()
total_by_category_age_18_25.show()
percentage_by_category_age_18_25.show()
top_3_categories.show()
