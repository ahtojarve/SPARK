import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

if __name__ == "__main__":

    #check the number of arguments
    #if len(sys.argv) != 3:
        #print("Usage: sql_example <input folder> <output folder>")
        #exit(-1)

    #Set a name for the application
    appName = "Businesses taht offer takeout and have menu photo with sql"

    #Set the input folder location to the first argument of the application
    #NB! sys.argv[0] is the path/name of the script file
    input_folder_photo = "yelp/small_photo"
    input_folder_business = "yelp/small_business"
    input_folder_review = "yelp/small_review"


    #create a new Spark application and get the Spark session object
    spark = SparkSession.builder.appName(appName).getOrCreate()

    #read in the JSON dataset as a DataFrame
    #inferSchema option forces Spark to automatically specify data column types
    photo = spark.read \
               .option("inferSchema", True) \
               .json(input_folder_photo)


    business = spark.read \
               .option("inferSchema", True) \
               .json(input_folder_business)

    review = spark.read \
               .option("inferSchema", True) \
               .json(input_folder_review)

    # Set the output folder location to the second argument of the application
    output_folder = "businesses_with_takeout_photo"

    #create a new Spark application and get the Spark session object
    spark = SparkSession.builder.appName(appName).getOrCreate()

    business_needed = business.select("business_id", "attributes.RestaurantsTakeOut")

    # Register DataFrame as a temporary SQL table
    photo.registerTempTable("photo")
    business_needed.registerTempTable("business_needed")
    review.registerTempTable("review")

    #Run an SQL statement. Result will be a DataFrame.
    vastus = spark.sql("""
    SELECT AVG(r.stars) AS average_stars
    FROM photo p
    JOIN business_needed b ON p.business_id = b.business_id
    JOIN review r ON p.business_id = r.business_id
    WHERE p.label = 'menu' AND b.RestaurantsTakeOut = True
""")


    #Register the resulting DataFreame as an SQL table so it can be addressed in following SQL querries.
    vastus.registerTempTable("vastus")

    #Display counts
    vastus.show()

    #Write results into the output folder
    vastus.write.format("json").save(output_folder)

    #To force Spark write output as a single file, you can use:
    #counts.coalesce(1).write.format("json").save(output_folder)
    #coalesce(N) repartitions DataFrame or RDD into N partitions.
    # NB! But be careful when using coalesce(N), your program will crash if the whole DataFrame does not fit into memory of N processes.

    #Stop Spark session
    spark.stop()