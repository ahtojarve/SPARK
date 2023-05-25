import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
#import pandas as pd

if __name__ == "__main__":

    #check the number of arguments
    if len(sys.argv) != 4:
        print("Usage: dataframe_example <photo folder> <business folder> <review folder>")
        exit(-1)

    #Set a name for the application
    appName = "Average amount of stars for Businesses that offer takeout and have a picture of the menu"

    #Set the input folder location to the first argument of the application
    #NB! sys.argv[0] is the path/name of the script file
    input_folder_photo = sys.argv[1]
    input_folder_business = sys.argv[2]
    input_folder_review = sys.argv[3]


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



    #Show dataset schema/structure with filed names and types
    business.printSchema()
    review.printSchema()
    photo.printSchema()

    

    
    
    business_needed = business.select("business_id", "attributes.RestaurantsTakeOut")
    #business_needed.show(10, True)


    joined_table = review.join(business_needed, "business_id").join(photo, "business_id")
    joined_table.show(10, True)

    answer_table = joined_table.filter((F.col("RestaurantsTakeOut") == True) & (F.col("label") == "menu"))
    #answer_table.show(10,False)

    #distinct_values = joined_table.select("label").distinct().collect()
    #for row in distinct_values:
        #print(row[0])
    average_amount = answer_table.groupBy().agg(F.avg("stars").alias("average_amount_of_stars"))
    result = average_amount.collect()[0][0]
    print(round(result,2))
    rounded_result = round(result, 2)

    #write results into csv file.

    with open ("Results/average_stars.csv", "w") as file:
        file.write(str(rounded_result))

    #Stop Spark session
    spark.stop()