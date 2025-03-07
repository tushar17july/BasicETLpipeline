import glob 
import pandas as pd 
import xml.etree.ElementTree as ET 
from datetime import datetime 

log_file = "log_file.txt" 
target_file = "transformed_data.csv" 

# Function to extract data from CSV files
def extract_from_csv(file_to_process): 
    dataframe = pd.read_csv(file_to_process) 
    return dataframe 

# Function to extract data from JSON files
def extract_from_json(file_to_process): 
    dataframe = pd.read_json(file_to_process, lines=True) 
    return dataframe 

# Function to extract data from XML files
def extract_from_xml(file_to_process): 
    dataframe = pd.DataFrame(columns=["name", "height", "weight"]) 
    tree = ET.parse(file_to_process) 
    root = tree.getroot() 
    
    # Loop through each person element in the XML and extract data
    for person in root: 
        name = person.find("name").text 
        height = float(person.find("height").text) 
        weight = float(person.find("weight").text) 
        
        # Append the extracted data to the dataframe
        dataframe = pd.concat([dataframe, pd.DataFrame([{"name": name, "height": height, "weight": weight}])], ignore_index=True) 
    return dataframe 

# Function to extract data from all supported file types
def extract(): 
    extracted_data = pd.DataFrame(columns=['name', 'height', 'weight'])  # Create an empty DataFrame to hold extracted data
    
    # Process all CSV files, except the target file
    for csvfile in glob.glob("*.csv"): 
        if csvfile != target_file:  # Ensure the file is not the target file to avoid overwriting
            extracted_data = pd.concat([extracted_data, extract_from_csv(csvfile)], ignore_index=True) 
    
    # Process all JSON files 
    for jsonfile in glob.glob("*.json"): 
        extracted_data = pd.concat([extracted_data, extract_from_json(jsonfile)], ignore_index=True) 
    
    # Process all XML files 
    for xmlfile in glob.glob("*.xml"): 
        extracted_data = pd.concat([extracted_data, extract_from_xml(xmlfile)], ignore_index=True) 
    
    return extracted_data 

# Function to transform data (convert height and weight)
def transform(data): 
    '''Convert inches to meters and round off to two decimals 
    1 inch is 0.0254 meters '''
    data['height'] = data['height'].apply(lambda x: round(x * 0.0254, 2)) 
 
    '''Convert pounds to kilograms and round off to two decimals 
    1 pound is 0.45359237 kilograms '''
    data['weight'] = data['weight'].apply(lambda x: round(x * 0.45359237, 2)) 
    
    return data 

# Function to load transformed data into a CSV file
def load_data(target_file, transformed_data): 
    transformed_data.to_csv(target_file, index=False)  # Save data without adding an index column

# Function to log progress of the ETL process
def log_progress(message): 
    timestamp_format = '%Y-%m-%d %H:%M:%S'  # Correct timestamp format 
    now = datetime.now()  # Get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    
    # Append log message with timestamp to the log file
    with open(log_file, "a") as f: 
        f.write(timestamp + ', ' + message + '\n') 

# Log the initialization of the ETL process 
log_progress("ETL Job Started") 

# Log the beginning of the Extraction process 
log_progress("Extract phase Started") 
extracted_data = extract() 

# Log the completion of the Extraction process 
log_progress("Extract phase Ended") 

# Log the beginning of the Transformation process 
log_progress("Transform phase Started") 
transformed_data = transform(extracted_data) 
print("Transformed Data") 
print(transformed_data) 

# Log the completion of the Transformation process 
log_progress("Transform phase Ended") 

# Log the beginning of the Loading process 
log_progress("Load phase Started") 
load_data(target_file, transformed_data) 

# Log the completion of the Loading process 
log_progress("Load phase Ended") 

# Log the completion of the ETL process 
log_progress("ETL Job Ended")
