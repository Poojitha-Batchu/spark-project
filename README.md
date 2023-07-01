# spark-project

URL: https://www.kaggle.com/datasets/new-york-city/nypd-motor-vehicle-collisions
• Understanding the dataset:
There are 2 relevant dataset files are files:
• MVCollisionsData Dictionary_20190813_ERD.xlsx - 98 KB
      -  This Excel file consists of 4 sheets - Database Info, Column Info, Diagram & Dataset Revision History
      -  Database Info sheet: Lists the 3 tables, namely,
• MV-Collisions - Crash: contains details on the Motor Vehicle Collisions crash event. Data for only this table is available.
  MV-Collisions - Vehicle: contains details on each vehicle involved in the crash. Nodata available
  MV-Collisions - Person: contains details for people involved in the crash. No data available.
  
o Column Info sheet: Contains column name, description, primary, foreign key details of the 3 tables.
o Diagram sheet: Entity relationship diagram of the 3 tables involved: Vehicles, Crashes &  Person
o Dataset Revision History sheet: Can be ignored
      nypd-motor-vehicle-collisions.csv-428 MB
o This data corresponds to the MV-Collisions - Crash table
o This table has 29 columns and contains the data that we will be working with be working with               

Problem Statement:
a) Data Cleansing:
    Data for some columns like Borough and Zip Code are missing. Re-create this missing data using columns Latitude and Longitude.
    Sub Tasks:
        1. Using Spark compute the number of rows in the file and save the output as a CSV file.
        2. Using Spark compute the total number of rows where the data is missing for Borough or Zip code
        3. Create a csv file with unique values for Latitude and Longitude (do not include the decimals) and corresponding Borough and Zip codes. Use this table to populate original     
            dataset with the missing Borough and Zip codes. Provide final count of rows with values still missing Borough and Zip codes and the count of rows where this data was added /                corrected. The rows with missing Latitude and Longitude should be ignored. The cleaned CSV file should not have any missing values for Borough, Zip Code, Latitude and         
            Longitude.
b) Data Insights using Spark:
    Read the corrected CSV data into Spark DataFrames and using the relevant transformations and actions generate CSV and Parquet files for the following insights:
    1.  Top 5 days that recorded the maximum accidents and the Boroughs where this accident occurred, and the vehicle type (Code 1) involved in these accidents
    2.  Which Borough recorded the maximum accidents and the top 5 days when this had occurred in the mentioned Borough
    3.  Top 5 vehicle type (Code 1) that was involved in maximum accidents
