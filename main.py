from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import pandas as pd

# Initialize SparkSession
spark = SparkSession.builder.appName("PatientDataGenerator").getOrCreate()

# Sample patient data (replace this with your actual table or data source)
data = [
    {"patient_id": 1234, "region": "North", "sales": 100, "year": 2021},
    {"patient_id": 1252, "region": "North", "sales": 150, "year": 2022},
    {"patient_id": 3672, "region": "South", "sales": 200, "year": 2021},
    {"patient_id": 43456, "region": "South", "sales": 250, "year": 2022},
    {"patient_id": 57896, "region": "East", "sales": 300, "year": 2021},
    {"patient_id": 6235, "region": "East", "sales": 350, "year": 2022},
    {"patient_id": 90128, "region": "East", "sales": -4, "year": 2022},
    {"patient_id": 0, "region": "East", "sales": 0, "year": 2022}
]

# Define the schema
schema = StructType([
    StructField("patient_id", IntegerType(), True),
    StructField("region", StringType(), True),
    StructField("sales", IntegerType(), True),
    StructField("year", IntegerType(), True)
])

# Create a DataFrame
df = spark.createDataFrame(data, schema)
df1 = pd.DataFrame(data)
df1["sales"] = df1["sales"].abs()

# Step 2: Retrieve rows where patient_id is 0 but sales is not 0
invalid_patient_data = df1[(df1['patient_id'] == 0) & (df1['sales'] != 0)]

# Step 3: Remove rows where patient_id is 0 and sales is also 0
df1 = df1[~((df1['patient_id'] == 0) & (df1['sales'] == 0))]
print(df1)

# Generator function to yield sales values for a specific region and year
def sales_generator(region, year, df):
    for row in df.filter((df.region == region) & (df.year == year)).collect():
        yield row["sales"]

region = 'North'
year = '2022'
sales_gen = sales_generator(region, year, df)
for i in sales_gen:
    print(i)

# Implement generator expression
total_sales = sum(sales for sales in sales_generator(region, year, df))
print(f"Total sales against each region '{region}': {total_sales}")

regions = [row["region"] for row in df.select("region").distinct().collect()]
print(regions)

# Calculate total sales for 2021 and 2022 using list comprehension
total_sales_2021 = sum([sales for sales in sales_generator(region, 2021, df)])
total_sales_2022 = sum([sales for sales in sales_generator(region, 2022, df)])

# Calculate YOY growth across all regions
yoy_growth = ((total_sales_2022 - total_sales_2021) / total_sales_2021) * 100

# Print YOY growth
print(f"YOY across all regions: {yoy_growth:.2f}%")

# Stop the SparkSession
spark.stop()
