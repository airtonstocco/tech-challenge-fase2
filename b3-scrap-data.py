import base64
import requests
import json
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from datetime import datetime

def lambda_handler(event, context):
    try:
        # 1. Coleta da API
        payload = {
            "language": "pt-br",
            "pageNumber": 1,
            "pageSize": 120,
            "index": "IBOV",
            "segment": "1"
        }
        encoded = base64.b64encode(json.dumps(payload).encode()).decode()
        url = f"https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetPortfolioDay/{encoded}"

        response = requests.get(url)
        if response.status_code != 200:
            return {
                "statusCode": 500,
                "body": f"Erro ao acessar a API: {response.status_code}"
            }

        data_json = response.json()
        if 'results' not in data_json or not data_json['results']:
            return {
                "statusCode": 204,
                "body": "API retornou sem dados."
            }

        data = data_json['results']
              
        # 2. Adiciona partição
        partition_date = datetime.today().strftime('%Y-%m-%d')  
        for row in data:
            row['partition_date'] = partition_date

        # 3. Salva como Parquet
        table = pa.Table.from_pylist(data)
        parquet_path = "/tmp/b3.parquet"
        pq.write_table(table, parquet_path)

        # 4. Upload para S3
        bucket_name = "b3-scrap-data"
        s3_key = f"raw/partition_date={partition_date}/b3.parquet"

        s3 = boto3.client('s3')
        s3.upload_file(parquet_path, bucket_name, s3_key)

        return {
            "statusCode": 200,
            "body": f"Arquivo enviado para: s3://{bucket_name}/{s3_key}"
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": f"Erro inesperado: {str(e)}"
        }