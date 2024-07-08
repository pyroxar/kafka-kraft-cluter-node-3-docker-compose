from kafka import KafkaProducer, KafkaConsumer
import json
import time

def test_kafka():
    # Konfiguracja producenta
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # Konfiguracja konsumenta
    consumer = KafkaConsumer(
        'test-topic',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    # Wysłanie wiadomości
    test_message = {'test': 'Wiadomość testowa'}
    producer.send('test-topic', test_message)
    producer.flush()
    print("Wysłano wiadomość:", test_message)

    # Odczytanie wiadomości
    print("Oczekiwanie na wiadomość...")
    for message in consumer:
        received_message = message.value
        print("Odebrano wiadomość:", received_message)
        if received_message == test_message:
            print("Test zakończony sukcesem!")
            break
        else:
            print("Nieoczekiwana wiadomość. Test nie powiódł się.")
            break

    producer.close()
    consumer.close()

if __name__ == "__main__":
    test_kafka()