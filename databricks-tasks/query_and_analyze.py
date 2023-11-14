import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import expr


def query_table(query):
    """
    Query a delta table using spark sql
    """
    try:
        res = spark.sql(query).collect()
        return res
    except Exception as e:
        # Print the error and return None or handle it as needed
        print(f"Error querying table: {e}")
        return None


# Initialize a Spark session
spark = SparkSession.builder.appName("DeltaTablesComparison").getOrCreate()

# Define paths for the Delta Lakes
delta_lake_path_superstar = "/dbfs/tables/nba-lake-superstar"
delta_lake_path_other = "/dbfs/tables/nba-lake-other"

# Load Delta tables into DataFrames
df_superstar = spark.read.format("delta").load(delta_lake_path_superstar)
df_other = spark.read.format("delta").load(delta_lake_path_other)


# SQL queries to count the total number of players in each Delta table
query_superstar = "SELECT COUNT(*) as total_players FROM delta.`{}`".format(delta_lake_path_superstar)
query_other = "SELECT COUNT(*) as total_players FROM delta.`{}`".format(delta_lake_path_other)

# Execute SQL queries
total_players_superstar = query_table(query_superstar)[0]["total_players"]
total_players_other =  query_table(query_other)[0]["total_players"]


# Define columns to compare
columns_to_compare = ["Starter", "Role_Player", "Bust"]
column_labels = ["Become a Starter", "Become a Role Player", "Bust"]

# Collect data from Spark DataFrames
data_superstar = [df_superstar.agg(expr(f"avg({col_name})").cast(DoubleType())).collect()[0][0] for col_name in columns_to_compare]
data_other = [df_other.agg(expr(f"avg({col_name})").cast(DoubleType())).collect()[0][0] for col_name in columns_to_compare]

# Plot double bar histogram
fig, ax = plt.subplots(figsize=(10, 6))

bar_width = 0.35
bar_positions_superstar = range(len(columns_to_compare))
bar_positions_other = [pos + bar_width for pos in bar_positions_superstar]

ax.bar(bar_positions_superstar, data_superstar, bar_width, label="High Superstar Chance", color='skyblue')
ax.bar(bar_positions_other, data_other, bar_width, label="Low Superstar Chance", color='salmon')

ax.set_xticks([pos + bar_width / 2 for pos in bar_positions_superstar])
ax.set_xticklabels(column_labels)
ax.legend()
ax.set_ylabel("Mean Probability")
ax.set_title("Comparison of Attributes between likely Superstars and Others")

plt.show()

# Print the total number of players in each Delta table
print(f"Total number of players in 'High Superstar Chance' table: {total_players_superstar}")
print(f"Total number of players in 'Low Superstar Chance' table: {total_players_other}")
