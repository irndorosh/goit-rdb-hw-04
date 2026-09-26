# Olist E-Commerce Data Engineering & Analytics Project

Цей проєкт присвячений розгортанню локальної бази даних PostgreSQL в середовищі Google Colab, завантаженню та обробці датасету **Olist Brazilian E-Commerce**, виконанню DDL/DML-операцій, а також побудові складних аналітичних SQL-запитів для розв'язання бізнес-задач та підготовки даних до ML (Feature Engineering).

---

## 📦 Джерело датасету, спосіб отримання та розмір вибірки (Sample Size)

* **Джерело даних:** [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) на Kaggle.
* **Спосіб отримання файлів:** Файли завантажуються безпосередньо в середовище Google Colab програмним шляхом за допомогою офіційної бібліотеки `kagglehub`.
* **Розмір вибірки (Sample Size):** У проєкті використовується повний датасет Olist, який охоплює інформацію про **100 000+ замовлень**, здійснених у Бразилії у період з 2016 по 2018 рік:
  * `olist_customers`: **99,441** записів
  * `olist_orders`: **99,441** записів
  * `olist_order_items`: **112,650** записів
  * `olist_products`: **32,951** записів
  * `olist_sellers`: **3,095** записів
  * `olist_order_reviews`: **99,224** записів

---

## 🗄 Список використаних таблиць

У базі даних `olist` створено та заповнено 6 основних таблиць:

1. **`olist_customers`** — дані про покупців (`customer_id`, `customer_unique_id`, zip-код, місто, штат).
2. **`olist_orders`** — інформація про замовлення (`order_id`, `customer_id`, статус замовлення, часові мітки покупки та доставки).
3. **`olist_order_items`** — позиції у замовленнях (`order_id`, `order_item_id`, `product_id`, `seller_id`, ціна, вартість доставки).
4. **`olist_products`** — каталог товарів (`product_id`, категорія товару, габарити та вага).
5. **`olist_sellers`** — інформація про продавців (`seller_id`, zip-код, місто, штат).
6. **`olist_order_reviews`** — відгуки покупців (`review_id`, `order_id`, числова оцінка `review_score`, тексти коментарів).

---

## 🚀 Автоматичне завантаження даних та інструкція із запуску

Нижче наведено повний Python-скрипт. Він автоматично розгортає DDL-схему (з Primary Key, Foreign Key та CHECK constraints), завантажує датасет з Kaggle через `kagglehub` та наповнює PostgreSQL даними.

### Python ETL Скрипт

```python
# --- КРОК 1: Ініціалізація PostgreSQL у Colab ---
import os

os.system("apt-get update -qq > /dev/null 2>&1")
os.system("apt-get install -y -qq postgresql postgresql-contrib > /dev/null 2>&1")
os.system("service postgresql start")

os.system("sudo -u postgres psql -c \"CREATE USER colab WITH PASSWORD 'colab';\"")
os.system("sudo -u postgres psql -c \"CREATE DATABASE olist OWNER colab;\"")
os.system("sudo -u postgres psql -c \"GRANT ALL PRIVILEGES ON DATABASE olist TO colab;\"")

# --- КРОК 2: Ваш ETL-скрипт ---
import shutil
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import kagglehub

# 1. Підключення до PostgreSQL
conn = psycopg2.connect("dbname=olist user=colab password=colab host=localhost port=5432")
cursor = conn.cursor()

# 2. Автоматичне створення DDL-схеми
ddl_script = """
CREATE TABLE IF NOT EXISTS olist_customers (
    customer_id VARCHAR(32) PRIMARY KEY,
    customer_unique_id VARCHAR(32) NOT NULL,
    customer_zip_code_prefix INT NOT NULL,
    customer_city VARCHAR(100) NOT NULL,
    customer_state VARCHAR(2) NOT NULL
);

CREATE TABLE IF NOT EXISTS olist_sellers (
    seller_id VARCHAR(32) PRIMARY KEY,
    seller_zip_code_prefix INT NOT NULL,
    seller_city VARCHAR(100) NOT NULL,
    seller_state VARCHAR(2) NOT NULL
);

CREATE TABLE IF NOT EXISTS olist_products (
    product_id VARCHAR(32) PRIMARY KEY,
    product_category_name VARCHAR(100),
    product_name_lenght INT CHECK (product_name_lenght >= 0),
    product_description_lenght INT CHECK (product_description_lenght >= 0),
    product_photos_qty INT CHECK (product_photos_qty >= 0),
    product_weight_g INT CHECK (product_weight_g >= 0),
    product_length_cm INT CHECK (product_length_cm >= 0),
    product_height_cm INT CHECK (product_height_cm >= 0),
    product_width_cm INT CHECK (product_width_cm >= 0)
);

CREATE TABLE IF NOT EXISTS olist_orders (
    order_id VARCHAR(32) PRIMARY KEY,
    customer_id VARCHAR(32) NOT NULL REFERENCES olist_customers(customer_id),
    order_status VARCHAR(20) NOT NULL CHECK (order_status IN ('delivered', 'invoiced', 'shipped', 'processing', 'unavailable', 'canceled', 'created', 'approved')),
    order_purchase_timestamp TIMESTAMP NOT NULL,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS olist_order_items (
    order_id VARCHAR(32) NOT NULL REFERENCES olist_orders(order_id) ON DELETE CASCADE,
    order_item_id INT NOT NULL,
    product_id VARCHAR(32) NOT NULL REFERENCES olist_products(product_id),
    seller_id VARCHAR(32) NOT NULL REFERENCES olist_sellers(seller_id),
    shipping_limit_date TIMESTAMP NOT NULL,
    price NUMERIC(10, 2) NOT NULL CHECK (price >= 0),
    freight_value NUMERIC(10, 2) NOT NULL CHECK (freight_value >= 0),
    PRIMARY KEY (order_id, order_item_id)
);

CREATE TABLE IF NOT EXISTS olist_order_reviews (
    review_id VARCHAR(32) NOT NULL,
    order_id VARCHAR(32) NOT NULL REFERENCES olist_orders(order_id) ON DELETE CASCADE,
    review_score INT NOT NULL CHECK (review_score BETWEEN 1 AND 5),
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date TIMESTAMP NOT NULL,
    review_answer_timestamp TIMESTAMP NOT NULL,
    PRIMARY KEY (review_id, order_id)
);
"""
cursor.execute(ddl_script)
conn.commit()
print("✅ DDL-схему успішно створено.")

# 3. Скачування датасету з Kaggle
path = kagglehub.dataset_download("olistbr/brazilian-ecommerce")

os.makedirs("data", exist_ok=True)
for file in os.listdir(path):
    if file.endswith(".csv"):
        shutil.copy(os.path.join(path, file), os.path.join("data", file))

# 4. Послідовне завантаження CSV-файлів у створені таблиці
file_mapping = {
    'olist_customers': 'data/olist_customers_dataset.csv',
    'olist_sellers': 'data/olist_sellers_dataset.csv',
    'olist_products': 'data/olist_products_dataset.csv',
    'olist_orders': 'data/olist_orders_dataset.csv',
    'olist_order_items': 'data/olist_order_items_dataset.csv',
    'olist_order_reviews': 'data/olist_order_reviews_dataset.csv'
}

for table, filepath in file_mapping.items():
    if os.path.exists(filepath):
        df = pd.read_csv(filepath)

        if table == 'olist_products':
            int_cols = [
                'product_name_lenght', 'product_description_lenght',
                'product_photos_qty', 'product_weight_g',
                'product_length_cm', 'product_height_cm', 'product_width_cm'
            ]
            for col in int_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

        df_records = df.astype(object).where(pd.notnull(df), None)

        columns = list(df_records.columns)
        query = f"INSERT INTO {table} ({','.join(columns)}) VALUES %s ON CONFLICT DO NOTHING;"

        values = [tuple(x) for x in df_records.to_numpy()]
        execute_values(cursor, query, values, page_size=5000)
        conn.commit()
        print(f"✅ Завантажено {len(df)} рядків у таблицю '{table}'")

cursor.close()
conn.close()
print("🎉 Усі дані успішно підготовлені та імпортовані у БД!")
