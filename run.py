from scripts import download, upload, transform, create_table
import logging
import yaml


def main():
    logging.info('Starting the data pipeline ingestion - Yellow Taxi Data')
    
    logging.info('Loading process parameters from config file.')

    with open("./conf/conf.yaml", "r") as file:
        config = yaml.safe_load(file)

    logging.info('✅ Parameters loaded.')

    logging.info(f'Downloading file from the date: {config['process-parameters']['data_ref']}')

    download.download_file(config['process-parameters']['data_ref'])

    logging.info(f'Uploading raw file to s3 bucket.')

    temp_file_name = f'./temp/{config['process-parameters']['file_prefix']}_{config['process-parameters']['data_ref']}.{config['process-parameters']['file_extension']}'

    upload.upload_file_to_bucket(temp_file_name, config['s3-parameters']['raw-bucket'])

    logging.info('✅ Upload complete.')

    logging.info(f'Starting transform task.')

    transform.data_transform(config['process-parameters']['file_prefix'], config['process-parameters']['file_extension'],  config['s3-parameters']['raw-bucket'], config['process-parameters']['data_ref'], list(config['schema'].keys()))

    logging.info('✅ Transform complete.')

    logging.info(f'Starting upload transformed file.')

    upload.upload_file_to_bucket(temp_file_name, config['s3-parameters']['prod-bucket'], f'{config['process-parameters']['partition_column']}={config['process-parameters']['data_ref']}')

    logging.info('✅ Upload complete.')

    logging.info(f'Starting create table or add partition step.')

    create_table.create_table_or_partition(config['process-parameters']['db'], config['process-parameters']['table_name'],  config['s3-parameters']['athena-bucket'], config['s3-parameters']['prod-bucket'], config['process-parameters']['data_ref'], config['schema'])

    logging.info(f'Table created.')

    logging.info('✅ Process completed.')


if __name__ == '__main__':
    main()




