import pandas as pd
from airflow.operators.python import PythonOperator

def run_transformation(**kwargs):
    try:
        # Pull the DataFrame from XCom
        ti = kwargs['ti']
        df = ti.xcom_pull(task_ids='run_extraction', key='extracted_data')

        if df is not None:
            # Transformation logic
            print(f"Data received for transformation with shape: {df.shape}")

            # Fill missing values
            df['AgencyID'] = df['AgencyID'].fillna(0.0)
            df['AgencyCode'] = df['AgencyCode'].fillna(0.0)

            # Remove irrational fiscal years
            df = df.drop(df[(df['FiscalYear'] == 1998) | (df['FiscalYear'] == 1999)].index)

            # Drop duplicates
            df = df.drop_duplicates()

            # Create tables
            fact_employee = df[['EmployeeID', 'AgencyID', 'FiscalYear', 'BaseSalary', 'RegularHours', 'RegularGrossPaid', 
                                'OTHours', 'TotalOTPaid', 'TotalOtherPay', 'PayBasis']]

            agency = df[['AgencyID', 'AgencyCode', 'AgencyName', 'AgencyStartDate', 'WorkLocationBorough']].copy().drop_duplicates().reset_index(drop=True)
            agency['AgencyStartDate'] = pd.to_datetime(agency['AgencyStartDate'])

            employee = df[['EmployeeID', 'TitleCode', 'LastName', 'FirstName', 'PayrollNumber', 'TitleDescription', 
                           'LeaveStatusasofJune30']].copy().drop_duplicates().reset_index(drop=True)

            # Push each transformed DataFrame to XCom for use in the next task
            kwargs['ti'].xcom_push(key='fact_employee', value=fact_employee.to_dict())
            kwargs['ti'].xcom_push(key='agency', value=agency.to_dict())
            kwargs['ti'].xcom_push(key='employee', value=employee.to_dict())

            print('Data transformation completed and pushed to XCom.')

        else:
            print('No data found to transform.')

    except Exception as e:
        print(f'An error occurred during transformation: {e}')
