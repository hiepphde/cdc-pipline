import json
import time

import faker
from confluent_kafka import SerializingProducer
import psycopg2
from datetime import datetime
import random

fake = faker.Faker()


# cdc.pipline
def generate_transaction():
    user = fake.simple_profile()
    return {
        "transactionId": fake.uuid4(),
        "userId": user['username'],
        "timestamp": datetime.utcnow().timestamp(),
        "amount": round(random.uniform(10, 1000), 2),
        "currency": random.choice(['USD', 'GBP']),
        "city": fake.city(),
        "country": fake.country(),
        "merchantName": fake.company(),
        "paymentMethod": random.choice(['credit_card', 'debit_card', 'online_transfer']),
        "ipAddress": fake.ipv4(),
        "voucherCode": random.choice(['', 'DISCOUNT10', '']),
        "affiliateId": fake.uuid4()
    }


# financial_transactions
def generate_sale_transaction():
    user = fake.simple_profile()
    return {
        "transactionId": fake.uuid4(),
        "productId": random.choice(['product1', 'product2', 'product3', 'product4', 'product5', 'product6']),
        "productName": random.choice(['laptop', 'mobile', 'tablet', 'watch', 'headphone', 'speaker']),
        'productCategory': random.choice(['electronic', 'fashion', 'grocery', 'home', 'beauty', 'sports']),
        'productPrice': round(random.uniform(10, 1000), 2),
        'productQuantity': random.randint(1, 10),
        'productBrand': random.choice(['apple', 'samsung', 'oneplus', 'mi', 'boat', 'sony']),
        'currency': random.choice(['USD', 'GBP']),
        'customerId': user['username'],
        'transactionDate': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
        "paymentMethod": random.choice(['credit_card', 'debit_card', 'online_transfer'])
    }


# cdc.pipline
def create_table(conn):
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id VARCHAR(255) PRIMARY KEY,
            user_id VARCHAR(255),
            timestamp TIMESTAMP,
            amount DECIMAL,
            currency VARCHAR(255),
            city VARCHAR(255),
            country VARCHAR(255),
            merchant_name VARCHAR(255),
            payment_method VARCHAR(255),
            ip_address VARCHAR(255),
            voucher_code VARCHAR(255),
            affiliateId VARCHAR(255)
        )
        """)
    cursor.close()
    conn.commit()


# cdc.pipline
def insert_transaction_postgres():
    conn = psycopg2.connect(
        host='localhost',
        database='financial_db',
        user='postgres',
        password='postgres',
        port=5432
    )

    create_table(conn)

    for i in range(1):
        transaction = generate_transaction()
        cur = conn.cursor()
        print(transaction)

        cur.execute(
            """
            INSERT INTO transactions(transaction_id, user_id, timestamp, amount, currency, city, country, merchant_name, payment_method, 
            ip_address, affiliateId, voucher_code)
            VALUES (%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (transaction["transactionId"], transaction["userId"],
                  datetime.fromtimestamp(transaction["timestamp"]).strftime('%Y-%m-%d %H:%M:%S'),
                  transaction["amount"], transaction["currency"], transaction["city"], transaction["country"],
                  transaction["merchantName"], transaction["paymentMethod"], transaction["ipAddress"],
                  transaction["affiliateId"], transaction["voucherCode"])
        )
        cur.close()
    conn.commit()


def delivery_reports(err, msg):
    if err is not None:
        print(f'Message transaction fail: {err}')
    else:
        print(f'Message transaction to {msg.topic} [{msg.partition()}]')


# financial_transactions
def main():
    topic = 'financial_transactions'
    producer = SerializingProducer({
        'bootstrap.servers': 'localhost:9092'
    })
    curr_time = datetime.now()

    while (datetime.now() - curr_time).seconds < 120:
        try:
            transaction = generate_sale_transaction()
            transaction['totalAmount'] = transaction['productPrice'] * transaction['productQuantity']
            print(transaction)

            producer.produce(topic,
                             key=transaction['transactionId'],
                             value=json.dumps(transaction),
                             on_delivery=delivery_reports
                             )
            producer.poll(0)
            time.sleep(5)
        except BufferError:
            print("Buffer full! Waiting...")
            time.sleep(1)
        except Exception as e:
            print(e)


if __name__ == "__main__":
    # main()
    insert_transaction_postgres();
