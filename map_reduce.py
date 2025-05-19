import lithops
import boto3
from insult_filter import filter_insult

def my_map_function(obj):
    print(f'I am processing the object {obj}')
    counter = {}

    # Extrae el bucket y la clave del objeto S3 desde la URI
    bucket_name = 'insults-bucket'
    key = obj.replace(f's3://{bucket_name}/', '')  # Extraer la clave del objeto desde la URI
    print(f'Found object: {key}')

    # Llamada para obtener el objeto de S3
    s3 = boto3.client('s3')
    data = s3.get_object(Bucket=bucket_name, Key=key)['Body'].read()

    # Procesa el contenido del archivo
    text = data.decode('utf-8')  # Decodificar el contenido del archivo como texto
    censored_text = filter_insult(text)

    # Cuenta los insultos en el texto censurado
    insult_count = 0
    for word in censored_text.split():
        if word == 'CENSORED':
            insult_count += 1

    return {
        'key': key,  # <-- include it in the result
        'censored_text': censored_text,
        'insult_count': insult_count
    }

def my_reduce_function(results):
    return {
        'total_insults': sum(r['insult_count'] for r in results),
        'censored_data': [{'key': r['key'], 'text': r['censored_text']} for r in results]
    }
