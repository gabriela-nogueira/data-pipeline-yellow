from io import StringIO
import pandas as pd
from datetime import datetime
import sys
import boto3
import dotenv

dotenv.load_dotenv()

def data_transform(file_prefix, file_extension, raw_bucket, dat_ref, columns_to_save):

    file_path = file_prefix + '_' + dat_ref + '.' + file_extension
    formatted_date = datetime.strptime(dat_ref, '%Y-%m').strftime('%Y%m')

    df = pd.read_parquet(f's3://{raw_bucket}/{file_path}')

    df['data_ref'] = formatted_date

    df_to_save = df[columns_to_save]

    df_to_save.to_parquet(f'./temp/{file_path}')

    return None


if __name__ == '__main__':
    file_prefix = sys.argv[1]
    file_extension = sys.argv[2]
    raw_bucket = sys.argv[3]    
    dat_ref = sys.argv[4]
    data_transform(file_prefix, file_extension, raw_bucket, dat_ref)



    