import pandas as pd
from airflow.models import Variable
from airflow.operators.python import PythonOperator

def run_extraction(**kwargs):
    try:
        def concatenated_datasets(datasets, axis=0):
            # Initialize an empty list to add read datasets
            loaded = []

            # Read datasets and append them to the list loaded
            for dataset in datasets:
                data = pd.read_csv(dataset)
                loaded.append(data)

            # Concatenate identical datasets
            final_concatenate = pd.concat(loaded, axis=axis, sort=False)
            return final_concatenate

        # Apply Function
        if __name__ == '__main__':
            datasets = [
                'https://raw.githubusercontent.com/Ahmed9667/End-to-End-NYC-PAYROLL-data-integration-Pipeline/refs/heads/main/Data/nycpayroll_2020.csv',
                'https://raw.githubusercontent.com/Ahmed9667/End-to-End-NYC-PAYROLL-data-integration-Pipeline/refs/heads/main/Data/nycpayroll_2021.csv'
            ]
            
        df = concatenated_datasets(datasets, axis=0)
        print(f"Data extracted successfully with shape: {df.shape}")
        kwargs['ti'].xcom_push(key='extracted_data', value=df)

    except Exception as e:
        print(f'An error occurred during extraction: {e}')
