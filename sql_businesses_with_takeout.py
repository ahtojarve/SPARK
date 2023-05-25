import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

if __name__ == "__main__":

    #Set a name for the application
    appName = "Businesses taht offer takeout and have menu photo with sql"

    #Set the input folders
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
    output_folder = "Results/average_stars_sql"

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

    vastus.show()

    #Write results into the output folder
    vastus.write.format("csv").save(output_folder)

    #Stop Spark session
    spark.stop()