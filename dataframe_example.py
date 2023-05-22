import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

if __name__ == "__main__":

    #check the number of arguments
    if len(sys.argv) != 2:
        print("Usage: dataframe_example <input folder> ")
        exit(-1)

    #Set a name for the application
    appName = "DataFrame Example"

    #Set the input folder location to the first argument of the application
    #NB! sys.argv[0] is the path/name of the script file
    input_folder = sys.argv[1]

    #create a new Spark application and get the Spark session object
    spark = SparkSession.builder.appName(appName).getOrCreate()

    #read in the JSON dataset as a DataFrame
    #inferSchema option forces Spark to automatically specify data column types
    review = spark.read \
               .option("inferSchema", True) \
               .json(input_folder)


    #Show 10 rows without truncating lines.
    #review content might be a multi-line string.
    review.show(10, False)

    #Show dataset schema/structure with filed names and types
    review.printSchema()

    #Group reviews by stars (1,2,3,4 or 5) and count how many there were inside each group. Result will be a DataFrame.
    counts = review.groupBy(review.stars).count()

    #Alternative way to aggregate values inside a group using the count(column) function from the pyspark.sql.functions library
    counts_alternative = review.groupBy("stars").agg(F.count("business_id"))

    #counting without grouping simply returns a global count (NB! output is no longer DataFrame)
    review_count = review.count()
    print("Total number of reviews:", review_count)


    #Display counts
    counts.show(5, False)
    counts_alternative.show(5, False)

    #Write results into the output folder
    #counts.write.format("json").save(output_folder)

    #To force Spark write output as a single file, you can use:
    #counts.coalesce(1).write.format("json").save(output_folder)
    #coalesce(N) repartitions DataFrame or RDD into N partitions.
    # NB! But be careful when using coalesce(N), your program will crash if the whole DataFrame does not fit into memory of N processes.

    #Stop Spark session
    spark.stop()