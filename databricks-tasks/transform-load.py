from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when
import os


def check_data_quality(df):
    """
    Use delta tables to do dome data quality checks
    """
    # Data quality checks
    null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).collect()

    # Check for duplicates based on all columns
    duplicate_rows = df.groupBy(df.columns).count().filter(col("count") > 1)

    # Print data quality report
    print("Data Quality Report:")
    print("===================")

    # Null values report
    print("\nNull Values Report:")
    for col_name, null_count in zip(df.columns, null_counts[0]):
        print(f"{col_name}: {null_count} null values")

    # Duplicates report
    print("\nDuplicates Report:")
    if duplicate_rows.count() == 0:
        print("No duplicate rows found.")
    else:
        duplicate_rows.show(truncate=False)


def write_to_delta(csv_file_path, delta_lake_path_superstar, delta_lake_path_other, overwrite=False):
    # Initialize a Spark session
    spark = SparkSession.builder.appName("CSVtoDelta").getOrCreate()

    # Load CSV file into a DataFrame
    df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

    # Check the darta quality using delta table functionality
    check_data_quality(df)

    # Split the DataFrame into two tables based on the "Superstar" column
    df_superstar = df.filter(col("Superstar") > 0.05)
    df_other = df.filter(col("Superstar") <= 0.05)

    # Check if Delta tables already exist
    delta_table_superstar_exists = os.path.exists(delta_lake_path_superstar)
    delta_table_other_exists = os.path.exists(delta_lake_path_other)

    # Write the tables to Delta Lakes conditionally
    if delta_table_superstar_exists and not overwrite:
        print(f"Delta table already exists at {delta_lake_path_superstar}. Skipping overwrite.")
    else:
        df_superstar.write.format("delta").mode("overwrite").save(delta_lake_path_superstar)

    if delta_table_other_exists and not overwrite:
        print(f"Delta table already exists at {delta_lake_path_other}. Skipping overwrite.")
    else:
        df_other.write.format("delta").mode("overwrite").save(delta_lake_path_other)

        

if __name__ == "__main__":
    csv_file_path = "/FileStore/csv/nba-draft.csv"
    delta_lake_path_superstar = "/dbfs/tables/nba-lake-superstar"
    delta_lake_path_other = "/dbfs/tables/nba-lake-other"

    write_to_delta(csv_file_path, delta_lake_path_superstar, delta_lake_path_other, overwrite=True)