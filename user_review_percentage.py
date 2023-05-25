import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

if __name__ == "__main__":

    #Set a name for the application
    appName = "User reviews and percentage of total"

    #Set the input folder location
    input_folder_review = "yelp/small_review"

    # Set the output folder location
    output_folder = "Results/user_review_percentage"

    #create a new Spark application and get the Spark session object
    spark = SparkSession.builder.appName(appName).getOrCreate()

    #read in the JSON dataset as a DataFrame
    #inferSchema option forces Spark to automatically specify data column types
    dataset = spark.read \
               .option("inferSchema", True) \
               .json(input_folder_review)


    #Show dataset schema/structure with filed names and types
    #dataset.printSchema()

    # Register DataFrame as a temporary SQL table
    dataset.registerTempTable("dataset")

    #Run an SQL statement. Result will be a DataFrame.
    counts = spark.sql("""
    SELECT user_id, stars, starcount, total, (starcount / total * 100) AS percent
    FROM (
        SELECT user_id, stars, COUNT(*) AS starcount, 
               SUM(COUNT(*)) OVER (PARTITION BY stars) AS total
        FROM dataset
        GROUP BY user_id, stars
    ) AS subquery
""")

    #Register the resulting DataFreame as an SQL table so it can be addressed in following SQL querries.
    counts.registerTempTable("counts")

    #Display counts
    counts.show()

    #Write results into the output folder
    counts.write.format("csv").save(output_folder)

    #Stop Spark session
    spark.stop()