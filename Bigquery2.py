#!/usr/bin/env python
# coding: utf-8

# In[1]:


# !pip install google-cloud-bigquery
# !pip install mysql-connector-python
# !pip install pandas
# !pip install pyarrow


# In[2]:


import mysql.connector
from google.cloud import bigquery
import os
import pandas as pd  # Import pandas for DataFrame

# Set the environment variable to point to your JSON credentials file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/wamp64/www/loadData2/credentials/googleCloud/plexiform_credential.json"

# Load the DataFrame into BigQuery
project_id = ""
dataset_id = ""

# Initialize the BigQuery client
client = bigquery.Client(project=project_id)

# Establish a connection to MySQL
try:
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database=""
    )

    if connection.is_connected():
        print("Connected to MySQL database")

        cursor = connection.cursor()

        # Get a list of all tables in the MySQL database
        cursor.execute("SHOW TABLES")
        tables = [table[0] for table in cursor.fetchall()]

        for table_name in tables:
            # Create a BigQuery table reference
            table_ref = client.dataset(dataset_id).table(table_name)

            # Configure the job to append data if the table already exists
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

            # Fetch the data from MySQL
            query = f"SELECT * FROM {table_name}"
            cursor.execute(query)
            
            # Convert the data to a DataFrame
            df = pd.DataFrame(cursor.fetchall(), columns=[i[0] for i in cursor.description])

            if not df.empty:
                # Load the data into BigQuery
                job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
                job.result()

                print(f"Loaded {len(df)} rows into BigQuery table {dataset_id}.{table_name}")

except mysql.connector.Error as error:
    print("Error connecting to MySQL database:", error)

finally:
    if 'cursor' in locals():
        cursor.close()
    if 'connection' in locals() and connection.is_connected():
        connection.close()
        print("Closed MySQL database connection")


# In[ ]:




