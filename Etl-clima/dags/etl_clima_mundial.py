from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

INPUT_FILE = "/opt/airflow/dags/weather_dataset.csv"
OUTPUT_FILE = "/opt/airflow/dags/weather_cleaned.csv"

def categorize_temp(temp):
    if temp < 15:
        return 'cold'
    elif temp < 25:
        return 'warm'
    else:
        return 'hot'

def classify_weather(desc):
    if "rain" in desc:
        return "rainy"
    elif "clear" in desc:
        return "clear"
    elif "cloud" in desc:
        return "cloudy"
    else:
        return "other"

def extract_data():
    """Leer el archivo CSV generado manualmente"""
    df = pd.read_csv(INPUT_FILE)
    # Imprime los primeros registros para asegurar que se está leyendo correctamente
    print("Datos extraídos:", df.head())
    return df.to_json(orient="records")  # Devuelve como JSON para pasar entre tareas

def store_city_avg_temperature(city, temperature):
    # Leer el archivo si existe, si no, crear uno nuevo
    try:
        city_avg_df = pd.read_csv('city_avg_temperature.csv')
    except FileNotFoundError:
        city_avg_df = pd.DataFrame(columns=['city', 'average_temperature', 'record_count'])

    # Comprobar si la ciudad ya existe
    if city in city_avg_df['city'].values:
        # Actualizar el promedio y el conteo
        old_avg = city_avg_df.loc[city_avg_df['city'] == city, 'average_temperature'].values[0]
        count = city_avg_df.loc[city_avg_df['city'] == city, 'record_count'].values[0]
        new_avg = ((old_avg * count) + temperature) / (count + 1)
        city_avg_df.loc[city_avg_df['city'] == city, 'average_temperature'] = new_avg
        city_avg_df.loc[city_avg_df['city'] == city, 'record_count'] = count + 1
    else:
        # Añadir nueva ciudad con el primer registro
        new_city_data = {'city': city, 'average_temperature': temperature, 'record_count': 1}
        city_avg_df = pd.concat([city_avg_df, pd.DataFrame([new_city_data])], ignore_index=True)

    # Guardar el dataframe actualizado
    city_avg_df.to_csv('city_avg_temperature.csv', index=False)


def transform_data(ti):
    """Transformación de datos incluyendo la ciudad y su temperatura promedio."""
    data = ti.xcom_pull(task_ids='extract_data')  # Recupera el output de la tarea extract_data

    if not data:
        print("No se encontraron datos para transformar.")
        return []

    print("Datos recibidos en la tarea transform_data:", data)

    data = pd.read_json(data)  # Convertir de JSON a DataFrame
    transformed_data = []

    for _, row in data.iterrows():
        city = row["city"].title()
        temp_c = row["temperature_c"]
        weather_desc = row["weather"]

        transformed_row = {
            "city": city,
            "temperature_celsius": round(temp_c, 2),
            "humidity": row["humidity_%"],
            "weather": weather_desc,
            "is_rainy": int("rain" in weather_desc.lower()),
            "climate_category": categorize_temp(temp_c),
            "classified_weather": classify_weather(weather_desc),
            "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        # Llamada a la función para almacenar las temperaturas promedio por ciudad
        store_city_avg_temperature(city, temp_c)

        transformed_data.append(transformed_row)

    return transformed_data

def load_data(ti):
    """Guardar el DataFrame transformado a un nuevo CSV"""
    transformed_data = ti.xcom_pull(task_ids='transform_data')  # Recupera el output de la tarea transform_data
    df = pd.DataFrame(transformed_data)
    df.to_csv(OUTPUT_FILE, index=False)

def get_sorted_city_avg():
    # Leer el archivo con el promedio de las ciudades
    try:
        city_avg_df = pd.read_csv('city_avg_temperature.csv')
    except FileNotFoundError:
        print("El archivo de promedios de ciudad no existe.")
        return []

    # Ordenar las ciudades por temperatura promedio de mayor a menor
    sorted_city_avg = city_avg_df.sort_values(by='average_temperature', ascending=False)

    return sorted_city_avg

def store_sorted_city_avg():
    sorted_city_avg = get_sorted_city_avg()
    if sorted_city_avg.empty:
        print("No hay datos para ordenar.")
    else:
        sorted_city_avg.to_csv('/opt/airflow/dags/sorted_city_avg_temperature.csv', index=False)
        print("Las ciudades se han ordenado y guardado correctamente.")

with DAG(
    'weather_etl_dag',
    default_args={'owner': 'airflow', 'retries': 1},
    description='ETL para obtener datos del clima',
    schedule_interval='@daily',  # Ejecuta el DAG todos los días
    start_date=datetime(2025, 4, 25),
    catchup=False,
) as dag:

    # Tareas del DAG
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
    )

    store_sorted_task = PythonOperator(
        task_id='store_sorted_city_avg',
        python_callable=store_sorted_city_avg
    )

    # Definir la secuencia de tareas
    extract_task >> transform_task >> load_task >> store_sorted_task
