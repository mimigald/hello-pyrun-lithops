import lithops
import boto3
from insult_filter import filter_insult

def my_map_function(obj):
    bucket_name = 'mimigald-bucket'
    key = obj.key  # <-- Save the key
    s3 = boto3.client('s3')
    data = s3.get_object(Bucket=bucket_name, Key=key)['Body'].read()

    text = data.decode('utf-8')
    censored_text = filter_insult(text)

    insult_count = sum(censored_text.lower().count(insult) for insult in blacklist)

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
