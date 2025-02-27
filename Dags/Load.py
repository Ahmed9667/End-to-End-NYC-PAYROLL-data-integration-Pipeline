import pandas as pd
from airflow.operators.python import PythonOperator

def run_load(**kwargs):
    ti = kwargs['ti']
    
    # Pull the DataFrames from XCom
    fact_employee = ti.xcom_pull(task_ids='run_transformation', key='fact_employee')
    agency = ti.xcom_pull(task_ids='run_transformation', key='agency')
    employee = ti.xcom_pull(task_ids='run_transformation', key='employee')
    
    # Convert the dictionary back to a DataFrame (if necessary)
    fact_employee_df = pd.DataFrame(fact_employee)
    agency_df = pd.DataFrame(agency)
    employee_df = pd.DataFrame(employee)

    from sqlalchemy import create_engine, text
    from dotenv import load_dotenv
    import os

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


    def create_tales():
    conn = db_connected()
    cursor = conn.cursor()
    query = """
                create schema if not exists payroll;


                CREATE TABLE IF NOT EXISTS payroll.agency(
                    AgencyID decimal(20,2),
                    AgencyCode decimal(20,2),
                    AgencyName varchar(255),
                    AgencyStartDate date,
                    WorkLocationBorough varchar(255));

                 CREATE TABLE IF NOT EXISTS payroll.fact_employee (
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
                

                 CREATE TABLE IF NOT EXISTS payroll.employee(
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

    def load_records(df):
    conn = db_connected()
    cursor = conn.cursor()

    for index, row in df.iterrows():
        try:
            cursor.execute("""
                INSERT INTO payroll.agency (AgencyID, AgencyCode,AgencyName, AgencyStartDate,WorkLocationBorough) 
                VALUES (%s, %s, %s, %s,%s);
            """, (row['AgencyID'], row['AgencyCode'], row['AgencyName'], row['AgencyStartDate'],row['WorkLocationBorough']))
        except psycopg2.IntegrityError:
            conn.rollback()  # Rollback on error (duplicate keys or constraint violations)
        else:
            conn.commit()  # Commit after each successful insert

    cursor.close()
    conn.close()
    load_records(agency)

    def load_records(df):
    conn = db_connected()
    cursor = conn.cursor()

    for index, row in df.iterrows():
        try:
            cursor.execute("""
                INSERT INTO payroll.fact_employee (EmployeeID, AgencyID, FiscalYear, BaseSalary, RegularHours, RegularGrossPaid, OTHours, TotalOTPaid, TotalOtherPay, PayBasis) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (row['EmployeeID'], row['AgencyID'], row['FiscalYear'], row['BaseSalary'], row['RegularHours'], row['RegularGrossPaid'], row['OTHours'], row['TotalOTPaid'], row['TotalOtherPay'], row['PayBasis']))
        except psycopg2.IntegrityError:
            conn.rollback()  
        else:
            conn.commit()  

    cursor.close()
    conn.close()
    load_records(fact_employee)

    def load_records(df):
    conn = db_connected()
    cursor = conn.cursor()

    for index, row in df.iterrows():
        try:
            cursor.execute("""
                INSERT INTO payroll.employee (EmployeeID , TitleCode ,LastName, FirstName,PayrollNumber,TitleDescription,LeaveStatusasofJune30) 
                VALUES (%s, %s, %s, %s,%s,%s,%s);
            """, (row['EmployeeID'], row['TitleCode'], row['LastName'], row['FirstName'],row['PayrollNumber'],row['TitleDescription'],row['LeaveStatusasofJune30']))
        except psycopg2.IntegrityError:
            conn.rollback()  
        else:
            conn.commit()  

    cursor.close()
    conn.close()
    load_records(employee)
