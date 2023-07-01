from pyspark.sql import *
from pyspark.sql.functions import round, col, trunc, first, count, desc, coalesce, format_number

if __name__ == "__main__":

    # Create SparkSession
    spark = SparkSession.builder.appName("NYC-Motor-Vehicle-Collisions") \
        .master("local[3]") \
        .getOrCreate()

    #--------------- (A) TASK-1 ------------------------------------------------------------------------------

    # Load dataset
    df = spark.read.format("csv").option("header", "true").load("data/nypd-motor-vehicle-collisions.csv.")
    #df.show()

    row_count = df.count()
    print("Total rows in csv file: ", row_count)

    output = spark.createDataFrame([(row_count,)], ["value"])
    output.write \
        .format("csv") \
        .mode("overwrite") \
        .option("path", "dataOutput/a/task-1") \
        .save()

    #--------------- (A) TASK-2 -----------------------------------------------------------------------------

    # Compute the total number of rows where Borough or Zip code is missing
    #num_missing_rows = spark.sql("select count(*) as Null from survey_table where BOROUGH is NULL and ZIP_CODE is NULL")

    num_missing_rows = df[(df.BOROUGH.isNull()) | (df.ZIP_CODE.isNull())].count()
    print("Null rows in BOROUGH and ZIP_CODE: ", num_missing_rows)

    #-------------- (A) TASK-3 (LONGITUDE AND LATITUDE without decimals ) --------------------------------------------------------------------------------

    # filter rows where Latitude or Longitude is missing
    df_clean = df[(df.LATITUDE.isNotNull() & df.LONGITUDE.isNotNull())]

    # convert Latitude and Longitude to integers
    df_clean = df_clean.withColumn("LATITUDE", format_number(df_clean['LATITUDE'].cast('int'), 0))
    df_clean = df_clean.withColumn("LONGITUDE", format_number(df_clean['LONGITUDE'].cast('int'), 0))

    # group by Latitude and Longitude to get unique values
    df_grouped = df_clean.groupBy("LATITUDE", "LONGITUDE").agg(
        first("BOROUGH").alias("BOROUGH_1"), first("ZIP_CODE").alias("ZIP_CODE_1")
    )
    #df_grouped.show()

    # save output as CSV file
    df_grouped.write.mode("overwrite").option("header", "true").csv("dataOutput/unique_lat_long.csv")

    # Load the cleaned locations dataframe with unique lat/long values
    locations_df = spark.read.format("csv").option("header", "true").load("dataOutput/unique_lat_long.csv")

    # Truncate the latitude and longitude values in the original dataframe
    df = df.withColumn("trunc_latitude", trunc(df.LATITUDE, '5')).withColumn("trunc_longitude", trunc(df.LONGITUDE, '5'))
    #df.show()

    # Join the two dataframes on their common columns
    joined_df = df.join(locations_df, ('df.trunc_latitude' == locations_df.LATITUDE) &
                        ('df.trunc_longitude' == locations_df.LONGITUDE), "left_outer")
    #joined_df.show()

    # Replace null values in the Borough and Zip Code columns with values from the locations dataframe
    updated_df = joined_df.withColumn("BOROUGH", coalesce(df['BOROUGH'], locations_df['BOROUGH_1'])) \
        .withColumn("ZIP_CODE", coalesce(df['ZIP_CODE'], locations_df['ZIP_CODE_1']))
    #updated_df.show()

    # Count the number of rows with missing Borough and Zip Code values before and after the update
    missing_count = updated_df[(updated_df["BOROUGH"].isNull() | updated_df['ZIP_CODE'].isNull())].count()
    updated_count = updated_df[(updated_df["BOROUGH"].isNotNull() & updated_df['ZIP_CODE'].isNotNull())].count()

    # Remove the rows where BOROUGH and ZIP_CODE is null
    df = df.dropna(subset=['BOROUGH', 'ZIP_CODE'])
    df.write.mode("overwrite").option("header", "true").csv("dataOutput/cleaned-data.csv")
    #df.show()

    print("Rows with missing values:", missing_count)
    print("Rows updated:", updated_count)

    # -------------------------(B) TASK-1 --------------------------------------------------------------

    # Group the data by Borough, date, and vehicle type (Code 1)
    # Count the number of accidents for each group and sort in descending order
    # Select the top 5 groups
    top_5_days = df.filter(col("VEHICLE_TYPE_CODE_1") != "UNSPECIFIED") \
        .groupBy("BOROUGH", "ACCIDENT_DATE", "VEHICLE_TYPE_CODE_1") \
        .agg(count("*").alias("ACCIDENTS")) \
        .orderBy(desc("ACCIDENTS")) \
        .select("ACCIDENT_DATE", "BOROUGH", "VEHICLE_TYPE_CODE_1", "ACCIDENTS") \
        .limit(5)
    print("-------------- TASK-1 -------------------")
    top_5_days.show()
    top_5_days.write.mode("overwrite").option("header", "true").csv("dataOutput/task1-output.csv")

    #--------------------------(B) TASK-2 --------------------------------------------------------------

    # Group the data by Borough and date
    # Count the number of accidents for each group and sort in descending order
    # Select the top group
    max_borough_accidents = df.filter(col("VEHICLE_TYPE_CODE_1") != "UNSPECIFIED") \
        .groupBy("BOROUGH", "ACCIDENT_DATE") \
        .agg(count("*").alias("ACCIDENTS")) \
        .orderBy(desc("ACCIDENTS")) \
        .limit(1)
    print("-------------- TASK-2 -------------------")
    max_borough_accidents.show()

    # Extract the name of the borough with the maximum accidents
    max_borough = max_borough_accidents.select("BOROUGH").collect()[0][0]

    # Group the data by date for the selected borough
    # Count the number of accidents for each group and sort in descending order
    # Select the top 5 groups
    top_5_days_borough = df.filter(col("VEHICLE_TYPE_CODE_1") != "UNSPECIFIED") \
        .filter(col("BOROUGH") == max_borough) \
        .groupBy("ACCIDENT_DATE") \
        .agg(count("*").alias("ACCIDENTS")) \
        .orderBy(desc("ACCIDENTS")) \
        .limit(5)
    top_5_days_borough.show()

    # Save the output as a CSV file
    top_5_days_borough.write.mode("overwrite").option("header", "true").csv("dataOutput/task2-output.csv")

    #--------------------------(B) TASK-3 --------------------------------------------------------------

    # Group the data by vehicle type (Code 1)
    # Count the number of accidents for each group and sort in descending order
    # Select the top 5 groups
    top_5_vehicle_types = df.filter(col("VEHICLE_TYPE_CODE_1") != "UNSPECIFIED") \
        .groupBy("VEHICLE_TYPE_CODE_1") \
        .agg(count("*").alias("ACCIDENTS")) \
        .orderBy(desc("ACCIDENTS")) \
        .limit(5)
    print("-------------- TASK-3 -------------------")
    top_5_vehicle_types.show()

    # Save the output as a csv file
    top_5_vehicle_types.write.mode("overwrite").option("header", "true").csv("dataOutput/task3-output.csv")
