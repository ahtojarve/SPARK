import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

if __name__ == "__main__":

    #check the number of arguments
    #if len(sys.argv) != 3:
    #    print("Usage: sql_example <input folder> <output folder>")
    #    exit(-1)

    #Set a name for the application
    appName = "Top 5 users who gave 5 star"

    #Set the input folder location to the first argument of the application
    #NB! sys.argv[0] is the path/name of the script file
    input_folder_review = "yelp/small_review"

    # Set the output folder location to the second argument of the application
    output_folder = "top_5_sql"

    #create a new Spark application and get the Spark session object
    spark = SparkSession.builder.appName(appName).getOrCreate()

    #read in the JSON dataset as a DataFrame
    #inferSchema option forces Spark to automatically specify data column types
    dataset = spark.read \
               .option("inferSchema", True) \
               .json(input_folder_review)


    #Show 10 rows without truncating lines.
    #dataset.show(10, True)

    #Show dataset schema/structure with filed names and types
    dataset.printSchema()

    # Register DataFrame as a temporary SQL table
    dataset.registerTempTable("dataset")

    #Run an SQL statement. Result will be a DataFrame.
    counts = spark.sql("Select user_id, stars, count(*) as starcount FROM dataset WHERE stars = 5 GROUP BY user_id, stars ORDER BY starcount DESC LIMIT 5; ")

    #Register the resulting DataFreame as an SQL table so it can be addressed in following SQL querries.
    counts.registerTempTable("counts")

    #Display counts
    counts.show()

    #Write results into the output folder
    counts.write.format("json").save(output_folder)

    #To force Spark write output as a single file, you can use:
    #counts.coalesce(1).write.format("json").save(output_folder)
    #coalesce(N) repartitions DataFrame or RDD into N partitions.
    # NB! But be careful when using coalesce(N), your program will crash if the whole DataFrame does not fit into memory of N processes.

    #Stop Spark session
    spark.stop()