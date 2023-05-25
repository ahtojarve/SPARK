import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col, concat_ws, year, month
from pyspark.sql.functions import avg
from pyspark.sql import Window

if __name__ == "__main__":

    #Set a name for the application
    appName = "DataFrame Example"

    #Set the input folder locations
    input_folder_business = "yelp/small_business"
    input_folder_review = "yelp/small_review"


    #create a new Spark application and get the Spark session object
    spark = SparkSession.builder.appName(appName).getOrCreate()

    #read in the JSON dataset as a DataFrame
    #inferSchema option forces Spark to automatically specify data column types
    review = spark.read \
               .option("inferSchema", True) \
               .json(input_folder_review)
    
    business = spark.read \
               .option("inferSchema", True) \
               .json(input_folder_business)
    
    business_needed = business.select("business_id", "city")
    #business_needed.show(10, True)

    joined_table = review.join(business_needed, "business_id")
    

    joined_table = joined_table.withColumn("year_month", concat_ws("-", year(col("date")), month(col("date"))))
    #joined_table.show(10, True)

    city_month = joined_table.select("city", "year_month", "stars")
    #city_month.show(10, True)

    df_avg = city_month.groupBy("city", "year_month").agg(avg("stars").alias("avg_stars")).orderBy("year_month")
    #df_avg.show(10, True)

    # Create a pivot table where year_month values become column headers
    window_spec = Window.partitionBy("city").orderBy("year_month")
    df_pivot = df_avg.withColumn("row_num", F.row_number().over(window_spec)).groupBy("city").pivot("year_month").avg("avg_stars")

    # Display the pivot table
    df_pivot.select("city", "2018-2", "2018-3", "2018-4", "2018-5", "2018-6", "2018-7").show(10, True)

    #Save the table
    df_pivot.write.format("csv").save("Results/average_stars_by_month")



    #Stop Spark session
    spark.stop()