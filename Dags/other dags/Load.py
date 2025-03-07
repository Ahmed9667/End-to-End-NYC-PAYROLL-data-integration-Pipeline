import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine
from airflow.operators.python import PythonOperator

def run_load(**kwargs):
    try:
        ti = kwargs['ti']

        # Pull the DataFrames from XCom
        fact_employee = ti.xcom_pull(task_ids='run_transformation', key='fact_employee')
        agency = ti.xcom_pull(task_ids='run_transformation', key='agency')
        employee = ti.xcom_pull(task_ids='run_transformation', key='employee')

        # Convert dictionaries back to DataFrame (if necessary)
        fact_employee_df = pd.DataFrame(fact_employee)
        agency_df = pd.DataFrame(agency)
        employee_df = pd.DataFrame(employee)

        # Use the Airflow PostgresHook to manage connections
        postgres_hook = PostgresHook(postgres_conn_id='nyc_default')  # Airflow will use the 'postgres_default' connection ID
        
        # Database connection parameters
        db_params = {
            'username': 'postgres',
            'password': 'ahly9667',
            'host': 'localhost',
            'port': '5432',
            'database': 'nyc'
        }


        def db_connected(database="nyc"):
                
                try:
                    conn = psycopg2.connect(
                        user=db_params['username'], 
                        host=db_params['host'], 
                        password=db_params['password'], 
                        port=db_params['port'], 
                        database=database
                    )
                    print(f"Successfully connected to database {database}.")
                    return conn
                except psycopg2.OperationalError as e:
                    print(f"Error while connecting to database {database}: {e}")
                    raise


        # Function to create tables in PostgreSQL
        def create_tables():
            # Connect to the database (this will use the Airflow connection credentials)
            conn = postgres_hook.get_conn()
            cursor = conn.cursor()

            query = """
                CREATE SCHEMA IF NOT EXISTS payroll;

                CREATE TABLE IF NOT EXISTS payroll.agency (
                    AgencyID decimal(20,2),
                    AgencyCode decimal(20,2),
                    AgencyName varchar(255),
                    AgencyStartDate date,
                    WorkLocationBorough varchar(255)
                );

                CREATE TABLE IF NOT EXISTS payroll.fact_employee (
                    EmployeeID int PRIMARY KEY,
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

                CREATE TABLE IF NOT EXISTS payroll.employee (
                    EmployeeID int PRIMARY KEY,
                    TitleCode int,
                    LastName varchar(255),
                    FirstName varchar(255),
                    PayrollNumber int,
                    TitleDescription varchar(255),
                    LeaveStatusasofJune30 varchar(255),
                    FOREIGN KEY (EmployeeID) REFERENCES payroll.fact_employee(EmployeeID)
                );
            """
            cursor.execute(query)
            conn.commit()
            cursor.close()
            conn.close()

        create_tables()

        # Function to load records into a table
        def load_records(df, table_name):
            # Connect to PostgreSQL
            conn = postgres_hook.get_conn()
            cursor = conn.cursor()

            for index, row in df.iterrows():
                try:
                    if table_name == "agency":
                        cursor.execute("""
                            INSERT INTO payroll.agency (AgencyID, AgencyCode, AgencyName, AgencyStartDate, WorkLocationBorough) 
                            VALUES (%s, %s, %s, %s, %s);
                        """, (row['AgencyID'], row['AgencyCode'], row['AgencyName'], row['AgencyStartDate'], row['WorkLocationBorough']))
                    elif table_name == "fact_employee":
                        cursor.execute("""
                            INSERT INTO payroll.fact_employee (EmployeeID, AgencyID, FiscalYear, BaseSalary, RegularHours, RegularGrossPaid, OTHours, TotalOTPaid, TotalOtherPay, PayBasis) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                        """, (row['EmployeeID'], row['AgencyID'], row['FiscalYear'], row['BaseSalary'], row['RegularHours'], row['RegularGrossPaid'], row['OTHours'], row['TotalOTPaid'], row['TotalOtherPay'], row['PayBasis']))
                    elif table_name == "employee":
                        cursor.execute("""
                            INSERT INTO payroll.employee (EmployeeID, TitleCode, LastName, FirstName, PayrollNumber, TitleDescription, LeaveStatusasofJune30) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s);
                        """, (row['EmployeeID'], row['TitleCode'], row['LastName'], row['FirstName'], row['PayrollNumber'], row['TitleDescription'], row['LeaveStatusasofJune30']))
                except Exception as e:
                    print(f"Error inserting data into {table_name}: {e}")
                    conn.rollback()  # Rollback on error
                else:
                    conn.commit()  # Commit after each successful insert
                    print(f"Data inserted into {table_name} successfully.")

            cursor.close()
            conn.close()

        # Load records into respective tables
        load_records(agency_df, "agency")
        load_records(fact_employee_df, "fact_employee")
        load_records(employee_df, "employee")

    except Exception as e:
        print(f"An error occurred during the load process: {e}")
