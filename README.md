# README

This is homework 3 in "data engineering course for conversion masters". The homework assignment can be found here: [courses.cs.ut.ee](https://courses.cs.ut.ee/2023/decm/spring/Main/BigDataLab). For this homework, I used a devcontainer that has Debian.

## DATA:
- [Data Link](https://owncloud.ut.ee/owncloud/s/ZEJbHL8NYAT5JKy) (password: andmetehnika)

## Python files

For every exercise in the homework, I created a `.py` file.

1. **dataframe_example.py**: An example Spark Python DataFrame script. You can run this to familiarize yourself with Spark. This file needs input arguments. For example, to run the program using the terminal, use the command: `python dataframe_example.py yelp/small_review`.
2. **find_top_5_users.py**: Finds out the top 5 users who gave out the most 5-star reviews. Writes this data into the "Results/TOP_5_DATAFRAME". To run this, use the command: `python find_top_5_users.py yelp/small_review`.
3. **businesses_with_takeout_photo.py**: Finds the average amount of stars for businesses that offer takeout and have a picture of the menu available. Writes the amount into the "Results/average_stars.csv" file. To run this, use the command: `python businesses_with_takeout_photo.py yelp/small_photo yelp/small_business yelp/small_review`.
4. **create_pivot_table.py**: Creates a Pivot table over cities and review date month and year, with average stars as the aggregation. Writes this data to "Results/average_stars_by_month". To run this, use the command: `python create_pivot_table.py`.
5. **sql_example.py**: An example Spark Python DataFrame script using SQL queries. You can run this to familiarize yourself with Spark using SQL. This file needs input arguments. For example, to run the program using the terminal, use the command: `python sql_example.py yelp/small_review`.
6. **top_5_sql.py**: Finds out the top 5 users who gave out the most 5-star reviews. Writes this data into the "Results/TOP_5_SQL". To run this, use the command: `python top_5_sql.py`.
7. **user_review_percentage.py**: For each user and amount of review stars S (S = 1, 2, 3, 4, or 5), computes the percentage of total S-star reviews that this person gave. Writes this data into the "Results/user_review_percentage". To run this, use the command: `python user_review_percentage.py`.
8. **sql_businesses_with_takeout.py**: Finds the average amount of stars for businesses that offer takeout and have a picture of the menu available. Writes this data into the "Results/average_stars_sql". To run this, use the command: `python sql_business_with_takeout.py`.
