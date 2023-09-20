#!/usr/bin/env python
# coding: utf-8

# In[1]:


import mysql.connector

# Connexion à la base de données
try:
    connection = mysql.connector.connect(
        host="localhost",    # Adresse du serveur MySQL
        user="root",  # Nom d'utilisateur MySQL
        password="",  # Mot de passe MySQL
        database=""  # Nom de la base de données MySQL
    )

    if connection.is_connected():
        print("Connecté à la base de données MySQL")

        # Crée un curseur pour exécuter des requêtes SQL
        cursor = connection.cursor()

        # Exemple de requête SQL
        query = "SELECT * FROM chantiers"

        # Exécute la requête SQL
        cursor.execute(query)

        # Récupère les résultats
        for row in cursor.fetchall():
            display(row)

except mysql.connector.Error as error:
    print("Erreur de connexion à la base de données:", error)

finally:
    # Ferme le curseur et la connexion à la base de données
    if 'cursor' in locals():
        cursor.close()
    if 'connection' in locals() and connection.is_connected():
        connection.close()
        # print("Connexion à la base de données MySQL fermée")


# In[2]:


import mysql.connector
import pandas as pd
from google.cloud import bigquery

# Establish a connection to MySQL and fetch data
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

        query = "SELECT * FROM exp_devis_susc_det"

        cursor.execute(query)

        # Fetch all rows into a list of tuples
        rows = cursor.fetchall()

except mysql.connector.Error as error:
    print("Error connecting to MySQL database:", error)
    rows = []

finally:
    if 'cursor' in locals():
        cursor.close()
    if 'connection' in locals() and connection.is_connected():
        connection.close()
        print("Closed MySQL database connection")

# Convert the MySQL data to a DataFrame
column_names = [desc[0] for desc in cursor.description]
df = pd.DataFrame(rows, columns=column_names)

display(df)




# In[3]:


import os

# Set the environment variable to point to your JSON credentials file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:\\Users\\reddy.massidisemi\\LoadDataBigQuery\\credentials\\googleCloud\\plexiform_credential.json"


# Load the DataFrame into BigQuery
project_id = ""
dataset_id = ""
table_id = ""

client = bigquery.Client(project=project_id)

# Specify the BigQuery table to load the data into
table_ref = client.dataset(dataset_id).table(table_id)

# Configure the job to append data if the table already exists
job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

# Load the DataFrame into BigQuery
job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)

# Wait for the job to complete
job.result()

print(f"Loaded {len(df)} rows into BigQuery table {project_id}.{dataset_id}.{table_id}")


# In[ ]:




