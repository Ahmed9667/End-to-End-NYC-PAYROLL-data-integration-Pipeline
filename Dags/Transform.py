import pandas as pd
from airflow.operators.python import PythonOperator

def run_transformation(**kwargs):
    # Pull the DataFrame from XCom
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='run_extraction', key='extracted_data')

    if df is not None:
        # As we see there are two columns with null values so we have to fill them
        df['AgencyID'] = df['AgencyID'].fillna(0.0)
        df['AgencyCode'] = df['AgencyCode'].fillna(0.0)

        # We will delete rows with fiscal years 1998 and 1999 because they are irrational
        df = df.drop(df[(df['FiscalYear'] == 1998) | (df['FiscalYear'] == 1999)].index)

        # Drop duplicates
        df = df.drop_duplicates()

        # Create table fact_employee
        fact_employee = df[['EmployeeID', 'AgencyID', 'FiscalYear', 'BaseSalary', 'RegularHours', 'RegularGrossPaid', 
                            'OTHours', 'TotalOTPaid', 'TotalOtherPay', 'PayBasis']]

        # Create agency table
        agency = df[['AgencyID', 'AgencyCode', 'AgencyName', 'AgencyStartDate', 'WorkLocationBorough']].copy().drop_duplicates().reset_index(drop=True)
        agency['AgencyStartDate'] = pd.to_datetime(agency['AgencyStartDate'])

        # Create employee table
        employee = df[['EmployeeID', 'TitleCode', 'LastName', 'FirstName', 'PayrollNumber', 'TitleDescription', 
                       'LeaveStatusasofJune30']].copy().drop_duplicates().reset_index(drop=True)

        # Push each transformed DataFrame to XCom for use in the next task
        kwargs['ti'].xcom_push(key='fact_employee', value=fact_employee.to_dict())
        kwargs['ti'].xcom_push(key='agency', value=agency.to_dict())
        kwargs['ti'].xcom_push(key='employee', value=employee.to_dict())

        print('Data transformation completed and pushed to XCom.')

    else:
        print('No data found to transform.')
