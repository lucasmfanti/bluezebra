import requests
import pandas as pd
import pytz
import json
import datetime as dt

from google.cloud import storage
from google.oauth2 import service_account

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def dataset_extraction():
    cities = [
        'Caçapava',
        'Igarata',
        'Jacarei',
        'Jambeiro',
        'Monteiro Lobato',
        'Paraibuna',
        'Santa Branca',
        'Sao Jose dos Campos',
        'Campos do Jordao',
        'Lagoinha',
        'Natividade da Serra',
        'Pindamonhangaba',
        'Redençao da Serra',
        'Santo Antonio do Pinhal',
        'Sao Bento do Sapucai',
        'Sao Luiz do Paraitinga',
        'Taubate',
        'Tremembe',
        'Aparecida',
        'Cachoeira Paulista',
        'Canas',
        'Cunha',
        'Guaratingueta',
        'Lorena',
        'Piquete',
        'Potim',
        'Roseira',
        'Arapei',
        'Areias',
        'Bananal',
        'Cruzeiro',
        'Lavrinhas',
        'Queluz',
        'Sao Jose do Barreiro',
        'Silveiras',
        'Caraguatatuba',
        'Ilhabela',
        'Sao Sebastiao',
        'Ubatuba'
    ]

    api_key = 'YOUR_API_KEY'
    country_code = 'br'

    data_list = []

    for i in range(len(cities)):
        url = f'http://api.openweathermap.org/geo/1.0/direct?q={cities[i]},{country_code}&limit=1&appid={api_key}'
        response = requests.get(url=url)
        data = {
            'id': i+1,
            'name': response.json()[0]['name'],
            'latitude': response.json()[0]['lat'],
            'longitude': response.json()[0]['lon'],
            'country_code': response.json()[0]['country'],
        }
        data_list.append(data)

    cities_df = pd.DataFrame(data_list)

    api_key = 'YOUR_API_KEY'

    data_list = []

    for i in range(len(cities_df)):
        lat = cities_df['latitude'][i]
        lon = cities_df['longitude'][i]
        url = f'http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={api_key}'
        response = requests.get(url=url)
        data = {
            'city_id': cities_df['id'][i],
            'air_quality': response.json()['list'][0]['main']['aqi'],
            'no': response.json()['list'][0]['components']['no'],
            'no2': response.json()['list'][0]['components']['no2'],
            'o3': response.json()['list'][0]['components']['o3'],
            'so2': response.json()['list'][0]['components']['so2'],
            'pm2_5': response.json()['list'][0]['components']['pm2_5'],
            'pm10': response.json()['list'][0]['components']['pm10'],
            'nh3': response.json()['list'][0]['components']['nh3'],
        }
        data_list.append(data)

    aqi_df = pd.DataFrame(data_list)

    api_key = 'YOUR_API_KEY'

    data_list = []

    for i in range(len(cities_df)):
        for j in range(len(cities_df)):
            if i != j:
                origin_lat = cities_df['latitude'][i]
                origin_lon = cities_df['longitude'][i]
                destination_lat = cities_df['latitude'][j]
                destination_lon = cities_df['longitude'][j]

                url = f'https://maps.googleapis.com/maps/api/directions/json?destination={destination_lat},{destination_lon}&origin={origin_lat},{origin_lon}&key={api_key}'

                response = requests.get(url=url)

                data = {
                    'origin_city_id': cities_df['id'][i],
                    'destination_city_id': cities_df['id'][j],
                    'distance': response.json()['routes'][0]['legs'][0]['distance']['value'],
                    'duration': response.json()['routes'][0]['legs'][0]['duration']['value']
                }
                data_list.append(data)

    routes_df = pd.DataFrame(data_list)

    now = dt.datetime.now(pytz.timezone('America/Sao_Paulo'))

    aqi_df['extracted_at'] = now
    routes_df['extracted_at'] = now

    agora = now.strftime('%Y%m%d_%H%M%S')

    cities_filename = 'cities.csv'
    aqi_filename = f'aqi_{agora}.csv'
    routes_filename = f'routes_{agora}.csv'

    cities_df.to_csv(cities_filename, index = False)
    aqi_df.to_csv(aqi_filename, index = False)
    routes_df.to_csv(routes_filename, index = False)

    bucket_name = "your-bucket"
    credentials_file_path = '/usr/local/airflow/include/credentials/credentials.json'

    credentials = service_account.Credentials.from_service_account_file(
        credentials_file_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    storage_client = storage.Client(credentials=credentials)
    bucket = storage_client.bucket(bucket_name)

    source_file_name = cities_filename
    destination_blob_name = f"bluezebra/cities/{cities_filename}"
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)

    source_file_name = aqi_filename
    destination_blob_name = f"bluezebra/aqi/{aqi_filename}"
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)

    source_file_name = routes_filename
    destination_blob_name = f"bluezebra/routes/{routes_filename}"
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)

    return 'success'

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2023, 6, 8),
    'concurrency': 1,
    'email': 'youremail@yourproivider.com',
    'email_on_failure': True,
    'email_on_retry': False
}

with DAG ('dataset_extraction',
         default_args=default_args,
         schedule_interval='0 * * * *',
         catchup=False
         ) as dag:
      opr_run = PythonOperator(task_id='dataset_extraction', python_callable = dataset_extraction)