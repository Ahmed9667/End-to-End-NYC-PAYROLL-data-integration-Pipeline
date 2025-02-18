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

We will first import the data from Github repository and create a scalable automated function to import two concatenated NYC dataframes:
``` python
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
```

Then We code to query about number of null values in wach column in the dataset:
```python
#Check for null values
cols= []
for i in df.columns:
    cols.append(i)

types_of_data = []
for i in df.columns:
    types_of_data.append(df[i].dtypes)

num_of_nulls = []
for i in df.columns:
    num_of_nulls.append(df[i].isnull().sum())

frame_of_nulls = pd.DataFrame()
frame_of_nulls['Column'] = cols
frame_of_nulls['Type of Data'] = types_of_data
frame_of_nulls['Number of Nulls'] = num_of_nulls
frame_of_nulls

```

and we fill null values to ensure integrity of data:
```python
df['AgencyID'] = df['AgencyID'].fillna(0.0)
df['AgencyCode'] = df['AgencyCode'].fillna(0.0)
```

For more consistency check we creat a head map code to visualize the null values if they are existed in the dataset
```python
#Check for null values
sns.heatmap(df.isnull().sum().to_frame().T )
plt.title('Number of Null Values')
plt.show()
```
![image](https://github.com/user-attachments/assets/b69d5fd7-f1ff-45ce-953e-7ac0a0b5d044)


Before converting data to into Second Normal Form (2NF),we need to ensure that the dataset is in First Normal Form (1NF) and then move it to 2NF so we will check for duplicated values and determine the candidtaed primary keys columns.
```python
#Drop Duplicates of data frame
df = df.drop_duplicates()

#Check for columns with duplicated and unique values to detect candidate primary keys
duplicates = []
for i in df.columns:
    duplicates.append(df[i].duplicated().any())

frame_of_duplicates = pd.DataFrame()
frame_of_duplicates['Column'] = list(df.columns)
frame_of_duplicates['Duplicated or Not'] = duplicates
frame_of_duplicates
```
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Column</th>
      <th>Duplicated or Not</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>FiscalYear</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1</th>
      <td>PayrollNumber</td>
      <td>True</td>
    </tr>
    <tr>
      <th>2</th>
      <td>AgencyID</td>
      <td>True</td>
    </tr>
    <tr>
      <th>3</th>
      <td>AgencyName</td>
      <td>True</td>
    </tr>
    <tr>
      <th>4</th>
      <td>EmployeeID</td>
      <td>False</td>
    </tr>
    <tr>
      <th>5</th>
      <td>LastName</td>
      <td>True</td>
    </tr>
    <tr>
      <th>6</th>
      <td>FirstName</td>
      <td>True</td>
    </tr>
    <tr>
      <th>7</th>
      <td>AgencyStartDate</td>
      <td>True</td>
    </tr>
    <tr>
      <th>8</th>
      <td>WorkLocationBorough</td>
      <td>True</td>
    </tr>
    <tr>
      <th>9</th>
      <td>TitleCode</td>
      <td>True</td>
    </tr>
    <tr>
      <th>10</th>
      <td>TitleDescription</td>
      <td>True</td>
    </tr>
    <tr>
      <th>11</th>
      <td>LeaveStatusasofJune30</td>
      <td>True</td>
    </tr>
    <tr>
      <th>12</th>
      <td>BaseSalary</td>
      <td>True</td>
    </tr>
    <tr>
      <th>13</th>
      <td>PayBasis</td>
      <td>True</td>
    </tr>
    <tr>
      <th>14</th>
      <td>RegularHours</td>
      <td>True</td>
    </tr>
    <tr>
      <th>15</th>
      <td>RegularGrossPaid</td>
      <td>True</td>
    </tr>
    <tr>
      <th>16</th>
      <td>OTHours</td>
      <td>True</td>
    </tr>
    <tr>
      <th>17</th>
      <td>TotalOTPaid</td>
      <td>True</td>
    </tr>
    <tr>
      <th>18</th>
      <td>TotalOtherPay</td>
      <td>True</td>
    </tr>
    <tr>
      <th>19</th>
      <td>AgencyCode</td>
      <td>True</td>
    </tr>
  </tbody>
</table>
</div>
We only have column 'EmployeeID' with unique keys

### Dimensional Model Of Schema

![Schema](https://github.com/user-attachments/assets/3bee5b4a-5b6e-435e-a32a-7c9a037ac2aa)


We will create tables according to the structured schema and then load them to PostgreSql server database
```python
#create table fact_employee
fact_employee= df[['EmployeeID','AgencyID','FiscalYear','BaseSalary','RegularHours', 'RegularGrossPaid', 'OTHours',
       'TotalOTPaid', 'TotalOtherPay','PayBasis']]

#create agency table
agency = df[['AgencyID','AgencyCode','AgencyName','AgencyStartDate', 'WorkLocationBorough']].copy().drop_duplicates().reset_index(drop=True)
agency['AgencyStartDate'] = pd.to_datetime(agency['AgencyStartDate'])

#create table employee
employee = df[['EmployeeID','TitleCode','LastName', 'FirstName','PayrollNumber','TitleDescription', 'LeaveStatusasofJune30']].copy().drop_duplicates().reset_index(drop=True)
```

### `2.Create a public user with limited privileges to enable public access to the NYC Data warehouse:`
For security purpose we will create a virtual environment with parameter of postgresql server with limited access
```python
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
'''
For security purpose we will create a virtual environment 
with parameter of postgresql server with limited access
'''
# Load the environment .env variables from the .env files
load_dotenv()
# parameters
host_db = os.getenv('host')
username_db = os.getenv ('user')
password_db = os.getenv ('password')
port_db = os.getenv ('port')
db_name = os.getenv ('name')

# Define database connection parameters including the database name
db_params = {
    'username':username_db,
    'password':password_db,
    'host':host_db,
    'port':port_db,
    'database':db_name

}

default_db_url =f"postgresql://{db_params['username']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/postgres"

#create database
try:
    # open the connection
    conn = psycopg2.connect(default_db_url)
    conn.autocommit = True
    cur = conn.cursor()

#check if the database is already existed
    cur.execute(f"select 1 from pg_catalog.pg_database where datname='{db_params['database']}'")
    exists = cur.fetchone()
    if not exists:
        # Create the database
        cur.execute(f"create database {db_params['database']}")
        print(f"Database {db_params['database']} created successfully")

    else:
        print(f"Database {db_params['database']} already existed")

    # Close the cnnection
    cur.close()
    conn.close()
except exception as e :
    print(f"an error {e} occurred")

# connect database
load_dotenv()
# parameters
host_db = os.getenv('host')
username_db = os.getenv ('user')
password_db = os.getenv ('password')
port_db = os.getenv ('port')
db_name = os.getenv ('name')
# Connect to the new created database nyc
def db_connected():
    connection = psycopg2.connect(user=username_db, 
                                   host=host_db,  
                                   password=password_db, 
                                   port=port_db,
                                   database=db_name)

    return connection

conn = db_connected()
print(f"Database {db_params['database']} connected successfully")
```

### `3.- Develop a scalable and automated ETL Pipeline to load the payroll data NYC data warehouse:`

```python
def create_tales():
    conn = db_connected()
    cursor = conn.cursor()
    query = """
                create schema if not exists payroll;

                create table payroll.agency(
                    AgencyID decimal(20,2),
                    AgencyCode decimal(20,2),
                    AgencyName varchar(255),
                    AgencyStartDate date,
                    WorkLocationBorough varchar(255));

                create table payroll.fact_employee(
                    EmployeeID int primary key,
                    AgencyID int,
                    FiscalYear int,
                    BaseSalary decimal(20,2),
                    RegularHours decimal(20,2),
                    RegularGrossPaid decimal(20,2),
                    OTHours decimal(20,2),
                    TotalOTPaid decimal(20,2),
                    TotalOtherPay decimal(20,2),
                    PayBasis varchar(255)
                );
                

                create table payroll.employee(
                    EmployeeID int primary key,
                    TitleCode int,
                    LastName varchar(255),
                    FirstName varchar(255),
                    PayrollNumber int,
                    TitleDescription varchar(255),
                    LeaveStatusasofJune30 varchar(255),
                    foreign key (EmployeeID) references payroll.fact_employee(EmployeeID)
                );
                    
            """
    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()

create_tales()
print('Schema and tables created successfully')
```

Showing Schema in the data warehouse

![image.png](attachment:image.png)

