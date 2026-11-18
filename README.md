# Amazon Delivery ETL Pipeline

A class-based Python ETL pipeline that extracts the Amazon Delivery dataset, cleans and transforms it with pandas, and loads it into Snowflake.

## Dataset

[Amazon Delivery Dataset on Kaggle](https://www.kaggle.com/datasets/sujalsuthar/amazon-delivery-dataset)

Download `amazon_delivery.csv` and place it in the same folder as the script. The file is not included in this repository.

## Pipeline Steps

**Extract**
- Detects the file encoding with `chardet`
- Re-encodes the file to UTF-8 and reads it into a pandas DataFrame

**Transform**
- Checks for and removes duplicate rows
- Checks that `Order_ID` is unique
- Checks for missing and unexpected columns
- Drops rows where every column is null
- Standardizes dates (`YYYY-MM-DD`) and times (`HH:MM:SS`)
- Merges order date and time into a single `Order_DateTime` column
- Converts the text placeholder `'NaN'` into real missing values

**Load**
- Loads the cleaned data into a Snowflake table

## Tech Stack

Python, pandas, chardet, Snowflake (Snowpark)

## How to Run

1. Install the requirements: `pip install pandas chardet snowflake-snowpark-python`
2. Download the dataset from the Kaggle link above.
3. Place `amazon_delivery.csv` in the same folder as the script and run it from that folder.
4. In `__init__`, fill in your own Snowflake connection details.
5. Run the script.


## Future Improvements

- Orchestrate the pipeline with Airflow and Docker
- Build a Tableau dashboard on the loaded Snowflake data