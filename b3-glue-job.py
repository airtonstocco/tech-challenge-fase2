import boto3

def lambda_handler(event, context):
    glue = boto3.client('glue')

    response = glue.start_job_run(
        JobName='b3glue'
    )

    return {
        'statusCode': 200,
        'body': f"Glue job iniciado com sucesso. ID: {response['JobRunId']}"
    }