# generate_and_insert_data.py

import pandas as pd
import random
from faker import Faker
from sqlalchemy import create_engine

# Inicializamos Faker
fake = Faker()

# Función para generar customers
def generate_customers(client, n):
    customers = []
    for _ in range(n):
        if client == "A":
            customers.append({
                "customer_id": fake.uuid4(),
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "email": fake.email(),
                "country": fake.country(),
                "created_at": fake.date_between(start_date='-2y', end_date='today')
            })
        elif client == "B":
            customers.append({
                "id": fake.uuid4(),
                "name": fake.name(),
                "email_address": fake.email(),
                "country_code": fake.country_code(),
                "registration_date": fake.date_between(start_date='-2y', end_date='today')
            })
        elif client == "C":
            customers.append({
                "customer_id": fake.uuid4(),
                "first_name": fake.first_name(),
                "email": fake.email()
                # datos faltantes
            })
    return pd.DataFrame(customers)

# Función para generar stores
def generate_stores(client):
    stores = []
    store_names = [
        "SuperStore New York", "SuperStore Madrid", "SuperStore Santiago",
        "SuperStore Berlin", "SuperStore Tokyo"
    ]
    countries = ["USA", "Spain", "Chile", "Germany", "Japan"]

    for i in range(1, 6):
        if client == "A":
            stores.append({
                "store_id": i,
                "store_name": store_names[i-1],
                "store_country": countries[i-1]
            })
        elif client == "B":
            stores.append({
                "id_store": i,
                "name_store": store_names[i-1],
                "country_store": countries[i-1]
            })
        elif client == "C":
            stores.append({
                "store_id": i,
                "store_name": store_names[i-1]
            })
    return pd.DataFrame(stores)

# Función para generar warehouses
def generate_warehouses(client):
    warehouses = []
    warehouse_names = [
        "Warehouse East Coast", "Warehouse Europe",
        "Warehouse LATAM", "Warehouse APAC"
    ]
    countries = ["USA", "Germany", "Chile", "Japan"]

    for i in range(1, 5):
        if client == "A":
            warehouses.append({
                "warehouse_id": i,
                "warehouse_name": warehouse_names[i-1],
                "warehouse_country": countries[i-1]
            })
        elif client == "B":
            warehouses.append({
                "id_warehouse": i,
                "name_warehouse": warehouse_names[i-1],
                "country_warehouse": countries[i-1]
            })
        elif client == "C":
            warehouses.append({
                "warehouse_id": i,
                "warehouse_name": warehouse_names[i-1]
                # Sin país para hacerlo más "sucio"
            })
    return pd.DataFrame(warehouses)

# Función para generar orders
def generate_orders(client, customers_df, stores_df, n):
    orders = []
    customer_ids = customers_df.iloc[:,0].tolist()
    store_ids = stores_df.iloc[:,0].tolist()
    for _ in range(n):
        if client == "A":
            orders.append({
                "order_id": fake.uuid4(),
                "customer_id": random.choice(customer_ids),
                "store_id": random.choice(store_ids),
                "order_date": fake.date_between(start_date='-1y', end_date='today')
            })
        elif client == "B":
            orders.append({
                "id_order": fake.uuid4(),
                "id_customer": random.choice(customer_ids),
                "id_store": random.choice(store_ids),
                "date_order": fake.date_between(start_date='-1y', end_date='today')
            })
        elif client == "C":
            orders.append({
                "order_id": fake.uuid4(),
                "customer_id": random.choice(customer_ids),
                "order_date": fake.date_between(start_date='-1y', end_date='today')
            })
    return pd.DataFrame(orders)

# Función para generar order_items
def generate_order_items(client, orders_df, warehouses_df, n):
    order_items = []
    order_ids = orders_df.iloc[:,0].tolist()
    warehouse_ids = warehouses_df.iloc[:,0].tolist()
    for _ in range(n):
        if client == "A":
            order_items.append({
                "order_item_id": fake.uuid4(),
                "order_id": random.choice(order_ids),
                "product_id": fake.uuid4(),
                "warehouse_id": random.choice(warehouse_ids),
                "quantity": random.randint(1, 5),
                "price": round(random.uniform(10, 500), 2)
            })
        elif client == "B":
            order_items.append({
                "id_item": fake.uuid4(),
                "id_order": random.choice(order_ids),
                "id_product": fake.uuid4(),
                "id_warehouse": random.choice(warehouse_ids),
                "qty": random.randint(1, 5),
                "item_price": round(random.uniform(10, 500), 2)
            })
        elif client == "C":
            order_items.append({
                "order_item_id": fake.uuid4(),
                "order_id": random.choice(order_ids),
                "product_id": fake.uuid4(),
                "warehouse_id": random.choice(warehouse_ids),
                "quantity": random.randint(1, 5)
                # Sin precio
            })
    return pd.DataFrame(order_items)

# Crear engines para bases de datos
def get_engine(db_name):
    return create_engine(f'postgresql+psycopg2://ecommerce_user:ecommerce_pass@localhost:5432/{db_name}')

# Main function
def generate_and_insert_all():
    client_databases = {
        "A": "ecommerce_client_a",
        "B": "ecommerce_client_b",
        "C": "ecommerce_client_c"
    }

    for client, db_name in client_databases.items():
        print(f"🔵 Generando e insertando datos para cliente {client} en base {db_name}...")

        customers = generate_customers(client, 500)
        stores = generate_stores(client)
        warehouses = generate_warehouses(client)
        orders = generate_orders(client, customers, stores, 10000)
        order_items = generate_order_items(client, orders, warehouses, 150)

        # Crear engine y conexión
        engine = get_engine(db_name)

        # Insertar datos
        if client == "A":
            customers.to_sql('customers_a', engine, if_exists='replace', index=False)
            stores.to_sql('stores_a', engine, if_exists='replace', index=False)
            warehouses.to_sql('warehouses_a', engine, if_exists='replace', index=False)
            orders.to_sql('orders_a', engine, if_exists='replace', index=False)
            order_items.to_sql('order_items_a', engine, if_exists='replace', index=False)
        elif client == "B":
            customers.to_sql('customers_b', engine, if_exists='replace', index=False)
            stores.to_sql('stores_b', engine, if_exists='replace', index=False)
            warehouses.to_sql('warehouses_b', engine, if_exists='replace', index=False)
            orders.to_sql('orders_b', engine, if_exists='replace', index=False)
            order_items.to_sql('order_items_b', engine, if_exists='replace', index=False)
        elif client == "C":
            customers.to_sql('customers_c', engine, if_exists='replace', index=False)
            stores.to_sql('stores_c', engine, if_exists='replace', index=False)
            warehouses.to_sql('warehouses_c', engine, if_exists='replace', index=False)
            orders.to_sql('orders_c', engine, if_exists='replace', index=False)
            order_items.to_sql('order_items_c', engine, if_exists='replace', index=False)

    print("✅ ¡Todos los datos generados e insertados correctamente!")

if __name__ == "__main__":
    generate_and_insert_all()
