import lithops
import boto3
from map_reduce import my_map_function, my_reduce_function

def main():
    bucket_name = 'mimigald-bucket'
    s3 = boto3.client('s3')

    # Listar los ficheros dentro del bucket
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix='')

    # Preparar iterdata con las URIs completas de los archivos S3
    iterdata = []
    for obj in response.get('Contents', []):
        key = obj['Key']
        print(f'Found object: {key}')
        iterdata.append(f's3://{bucket_name}/{key}')  # Agregar URI

    print(iterdata)

    # Creación de un ejecutor de funciones de Lithops
    fexec = lithops.FunctionExecutor(log_level='INFO')

    # Ejecución el proceso de map-reduce
    fexec.map_reduce(my_map_function, iterdata, my_reduce_function)

    # Obtención el resultado final
    result = fexec.get_result()
    print("Censorship complete!")
    print(f"Total number of insults found: {result['total_insults']}")

    # Subir los ficheros con los resultados al bucket de S3
    for item in result['censored_data']:
        censored_file_key = f"censored_{item['key']}"
        censored_text = item['text']
        
        # Me aseguro de terminar con \n si es necesario
        if not censored_text.endswith('\n'):
            censored_text += '\n'
            
        s3.put_object(Bucket=bucket_name, Key=censored_file_key, Body=censored_text)

    print("Censored texts uploaded to S3.")

if __name__ == "__main__":
    main()
