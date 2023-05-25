
import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

if __name__ == "__main__":

    #check the number of arguments
    if len(sys.argv) != 2:
        print("Usage: dataframe_example <input folder review>")
        exit(-1)

    #Set a name for the application
    appName = "Find top 5 users who gave the most 5 star reviews"

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



    #Show dataset schema/structure with filed names and types
    review.printSchema()

    #Alternative way to aggregate values inside a group using the count(column) function from the pyspark.sql.functions library
    counts = review.groupBy("user_id", "stars").agg(F.count("*") \
                                                .alias("starcount")).filter(F.col("stars") == 5) \
                                                .orderBy(F.desc("starcount"))



    #Display counts
    #counts.show(5, False)
    counts.show(5, False)
    top_5 = counts.limit(5)

    #Write results into the output folder
    #top_5.write.format("csv").save("top_5")
    #To force Spark write output as a single file, you can use:
    top_5.coalesce(1).write.format("csv").save("Results/TOP_5_DATAFRAME") #millegi pärast ei töödanud


    #Stop Spark session
    spark.stop()