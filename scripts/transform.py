from io import StringIO
import pandas as pd
from datetime import datetime
import sys
import boto3
import dotenv

dotenv.load_dotenv()

def data_transform(file_prefix, file_extension, source_bucket, dat_ref):

    file_path = file_prefix + '_' + dat_ref + '.' + file_extension
    formatted_date = datetime.strptime(dat_ref, '%Y-%m').strftime('%Y%m')

    df = pd.read_parquet(f's3://{source_bucket}/{file_path}')

    columns_to_save = ['VendorID', 'passenger_count', 'total_amount', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']

    df_to_save = df[columns_to_save]

    df_to_save['data_ref'] = formatted_date

    df_to_save.to_parquet(f'../temp/{file_path}')

    return None


if __name__ == '__main__':
    file_prefix = sys.argv[1]
    file_extension = sys.argv[2]
    source_bucket = sys.argv[3]    
    dat_ref = sys.argv[4]
    transformacao_dos_dados(file_prefix, file_extension, source_bucket, dat_ref)



    