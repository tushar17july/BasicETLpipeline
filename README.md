ETL Script

Overview

This ETL (Extract, Transform, Load) script processes CSV, JSON, and XML files, extracts data, transforms it, and loads the transformed data into a CSV file. The script also logs the progress of each ETL phase.

This project is part of the edX X IBM Professional Certificate in Data Engineering Fundamentals.

Features

Extracts data from CSV, JSON, and XML files

Converts height from inches to meters

Converts weight from pounds to kilograms

Saves transformed data to a CSV file

Logs each phase of the ETL process

Requirements

Python 3.x

Required Libraries:

pandas

glob

xml.etree.ElementTree

datetime

Installation

Ensure Python 3.x is installed.

Install dependencies using pip:

pip install pandas

Place the script in the directory containing the CSV, JSON, and XML files.

Usage

Run the script in the terminal or command prompt:

python etl_script.py

The extracted and transformed data will be saved in transformed_data.csv.

Log details will be recorded in log_file.txt.

File Processing Details

CSV Files: Reads all .csv files except transformed_data.csv.

JSON Files: Reads all .json files.

XML Files: Reads all .xml files and extracts name, height, and weight.

Output

Transformed Data File: transformed_data.csv

Log File: log_file.txt

Logging Details

The script logs each step of the ETL process, including:

ETL start and end times

Extraction start and completion

Transformation start and completion

Data loading start and completion

Example Log Entry

2025-03-08 12:34:56, ETL Job Started
2025-03-08 12:34:57, Extract phase Started
2025-03-08 12:35:02, Extract phase Ended
2025-03-08 12:35:03, Transform phase Started
2025-03-08 12:35:05, Transform phase Ended
2025-03-08 12:35:06, Load phase Started
2025-03-08 12:35:07, Load phase Ended
2025-03-08 12:35:08, ETL Job Ended

Customization

Modify target_file to change the output filename.

Update the transformation functions to include additional data processing.

Author

Developed by [Your Name]
