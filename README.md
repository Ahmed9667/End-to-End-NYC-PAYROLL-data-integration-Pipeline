#  NYC PAYROLL DATA INTEGRATION Project

## Project Introduction:
This is a project to integrate payroll data across all its recorded agencies.
The City of New York is embarking on a project to integrate payroll data across all its 
agencies. The City of New York would like to develop a Data Analytics platform to accomplish two primary objectives:
- Financial Resource Allocation Analysis: Analyze how the City's financial resources are allocated and how much of the City's budget is being devoted to overtime.
- Transparency and Public Accessibility: Make the data available to the interested public to show how the City’s budget is being spent on salary and overtime pay for all municipal employees.

## Tech Stack:
- Python(Pndas, Matplotlib, Seaborn)
- PostgreSQL
- Git Version Controll

## Data Architecture:

![tttttt drawio](https://github.com/user-attachments/assets/042cdeda-f566-452d-90b3-98fc0716e84a)


## Project's Objectives:
- Ensure quality and consistency of data in your pipeline
- Create a public user with limited privileges to enable public access to the NYC Data warehouse
- Develop a scalable and automated ETL Pipeline to load the payroll data NYC data warehouse
- Develop aggregate table(s) in the Data warehouse for easy analysis of the key business questions


### `1.Ensure quality and consistency of data in your pipeline:`
To check for any data validation for analysis and ETL process we must cleans the data and make sure that it has no null values or duplicated records and ensure the validity of rational data types's columns according to the business keys.

We will first import the data from Github repository and create a scalable automated function to import two concatenated NYC dataframes
'''python
# Create a scalable function to read identitcal datasets in pandas
def concatenated_datasets(datasets , axis= 0):
    # initialize empty list to add read datasets
    loaded = []

    #read datasets and append them to the list loaded
    for dataset in datasets:
        data = pd.read_csv(dataset)
        loaded.append(data)

    #Comcatenate identitcal datasets
    final_concatenate = pd.concat(loaded , axis=0 , sort=False)
    return final_concatenate

#Apply Function
if __name__ == '__main__':
    datasets = ['https://raw.githubusercontent.com/Ahmed9667/End-to-End-NYC-PAYROLL-data-integration-Pipeline/refs/heads/main/Data/nycpayroll_2020.csv',
                'https://raw.githubusercontent.com/Ahmed9667/End-to-End-NYC-PAYROLL-data-integration-Pipeline/refs/heads/main/Data/nycpayroll_2021.csv']
    
df =   concatenated_datasets(datasets , axis=0) 
print(df)
'''


  
