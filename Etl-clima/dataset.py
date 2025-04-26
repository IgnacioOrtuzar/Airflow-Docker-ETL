import pandas as pd
import random
from datetime import datetime, timedelta

# Configuración
NUM_ROWS = 10000
START_DATE = datetime(2024, 1, 1)
CITIES = [
    {"city": "Auckland", "country": "NZ"},
    {"city": "Wellington", "country": "NZ"},
    {"city": "Christchurch", "country": "NZ"},
    {"city": "Hamilton", "country": "NZ"},
    {"city": "Tauranga", "country": "NZ"},
    {"city": "Napier", "country": "NZ"},
    {"city": "Dunedin", "country": "NZ"},
    {"city": "Palmerston North", "country": "NZ"},
]
WEATHER_TYPES = ["Sunny", "Cloudy", "Rainy", "Stormy", "Foggy", "Snowy"]

# Generación de datos
data = []
for i in range(NUM_ROWS):
    date = START_DATE + timedelta(hours=i)
    location = random.choice(CITIES)
    temp = round(random.uniform(5.0, 30.0), 1)
    humidity = random.randint(40, 100)
    wind_speed = round(random.uniform(0.5, 40.0), 1)
    weather = random.choice(WEATHER_TYPES)

    data.append({
        "id": i + 1,
        "date": date.strftime("%Y-%m-%d %H:%M:%S"),
        "city": location["city"],
        "country": location["country"],
        "temperature_c": temp,
        "humidity_%": humidity,
        "wind_speed_kmh": wind_speed,
        "weather": weather
    })

# Guardar como CSV
df = pd.DataFrame(data)
df.to_csv("weather_dataset.csv", index=False)

print("✅ Dataset generado exitosamente: weather_dataset.csv")