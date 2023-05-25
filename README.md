#README

This is homework 3 in "data engineering course for conversion masters". The homework assignment can be found here:[courses.cs.ut.ee] (https://courses.cs.ut.ee/2023/decm/spring/Main/BigDataLab). For this homework I used devcontainer that has debian. 
#DATA:
https://owncloud.ut.ee/owncloud/s/ZEJbHL8NYAT5JKy. (password:andmetehnika)

#Pyhthon files
For every excercise in the homework I created a .py file

1. **dataframe_example.py** is an example Spark Python DataFrame script. You can run this to familiarize yourself with spark. This file need input arguments, for example  I ran the program using terminal and wrote "python dataframe_example.py yelp/small_review". 
2. **find_top_5_users.py** Finds out the top 5 users who gave out the most 5 star reviews. Writes this data into the "Results/TOP_5_DATAFRAME"(run this in terminal: python find_top_5_users.py yelp/small_review)
3. **businesses_with_takeout_photo.py** Finds the average amount of stars for Businesses that offer takeout and have a picture of the menu available. Writes the amount into the "Results/average_stars.csv" file. (run this in terminal: python businesses_with_takeout_photo.py yelp/small_photo yelp/small_business yelp/small_review)
4. **create_pivot_table.py** Creates a Pivot table over cities and review date month and year, with average stars as the aggregation. Writes this data to "Results/average_stars_by_month" (run this in terminal: python create_pivot_table.py )
5. **sql_example.py** is an example Spark Python DataFrame script. You can run this to familiarize yourself with spark using sql sentences. This file need input arguments, for example  I ran the program using terminal and wrote "python sql_example.py yelp/small_review". 
6. **top_5_sql.py** Finds out the top 5 users who gave out the most 5 star reviews. Writes this data into the "Results/TOP_5_SQL"(run this in terminal: python top_5_sql.py )
7. **user_review_percentage.py** For each user and amount of review stars S (S = 1,2,3,4 or 5) - computes the percentage of total S-star reviews that this person gave. Writes this data into the "Results/user_review_percentage" (run this in terminal: python user_review_percentage.py)
8. **sql_businesses_with_takeout.py** Finds the average amount of stars for Businesses that offer takeout and have a picture of the menu available. Writes this data into the "Results/average_stars_sql" (run this in terminal: sql_business_with_takeout.py)
