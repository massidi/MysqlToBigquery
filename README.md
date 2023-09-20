# Loading Data from MySQL to BigQuery

This document explains a Python script for loading data from a MySQL database table into Google BigQuery. The script uses the `google-cloud-bigquery` and `mysql-connector-python` libraries to establish connections and perform the data transfer.

### Prerequisites

1. Ensure you have the necessary Python libraries installed:
   - `google-cloud-bigquery`
   - `mysql-connector-python`

2. You need a Google Cloud Service Account Key JSON file, which you've set as the environment variable `GOOGLE_APPLICATION_CREDENTIALS`. This service account should have the necessary permissions to interact with BigQuery.

3. Manually create BigQuery tables that match the schema of your MySQL database tables before running this script.

### Code Explanation

1. **Setting Google Cloud Credentials**

   The script starts by setting the Google Cloud credentials using the `os.environ` command to specify the path to your service account JSON file.

2. **Initialization**

   - `project_id` and `dataset_id` should be replaced with your Google Cloud project and dataset IDs.

   - The script initializes the BigQuery client using the `bigquery.Client` class.

3. **MySQL Connection**

   - The script establishes a connection to your MySQL database running locally (adjust host, user, password, and database as needed).

   - If the connection is successful, it prints a confirmation message.

4. **Fetching MySQL Tables**

   - The script executes `SHOW TABLES` to retrieve a list of all tables in the MySQL database.

   - It stores the table names in the `tables` list.

5. **Data Transfer Loop**

   - The script iterates through each table in the `tables` list.

   - For each table:
     - It creates a BigQuery table reference using the `client.dataset(dataset_id).table(table_name)` method.
     - Configures the job to append data (`WRITE_TRUNCATE` mode) if the table already exists.
     - Fetches the data from MySQL using a `SELECT *` query.
     - Converts the fetched data into a Pandas DataFrame.
     - Checks if the DataFrame is not empty.
     - If not empty, loads the data into BigQuery using the `client.load_table_from_dataframe` method and waits for the job to complete.

6. **MySQL Connection Cleanup**

   - In the `finally` block, it ensures that the MySQL cursor and connection are properly closed.

### Running the Script

1. Make sure you've satisfied the prerequisites mentioned above.

2. Manually create BigQuery tables that match the schema of the corresponding MySQL tables before running this script.

3. Replace `project_id` and `dataset_id` with your actual project and dataset information.

4. Execute the script, and it will load data from MySQL tables into BigQuery tables.

**Note:** Ensure that the BigQuery tables are created manually with schemas that match the corresponding MySQL tables. The script assumes the tables exist and will load data into them.
