from confluent_kafka import Consumer, KafkaError
import json
import csv
import os
from datetime import datetime

config = {
    'bootstrap.servers': 'kafka:29092',
    'group.id': 'flights_csv_consumer',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': 'false',
    'session.timeout.ms': '6000'
}

consumer = Consumer(config)

TOPIC = 'flights-connector-v1.flights.flights'

CSV_PATH = 'data/raw/flights.csv'


def ensure_csv_exists():
    headers = [
        'id', 'flight_number', 'origin', 'destination',
        'departure_time', 'arrival_time', 'aircraft_type',
        'created_at', 'event_timestamp', 'operation'
    ]
    
    if not os.path.exists(CSV_PATH):
        os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)
        with open(CSV_PATH, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(headers)


def convert_timestamp(ts_ms):
    if ts_ms:
        return datetime.fromtimestamp(ts_ms/1000).isoformat()
    return None


def process_message(msg):
    try:
        if msg.value() is None:
            print("Mensagem recebida sem valor (None)")
            return
            
        value = json.loads(msg.value().decode('utf-8'))
        print("Mensagem recebida:")
        print(json.dumps(value, indent=2))
        
        payload = value.get('payload', {})
        operation = payload.get('op')
        
        data = payload.get(
            'after' if operation in ['c', 'u'] else 'before',
            {}
        )
        
        if not data:
            print(f"Nenhum dado encontrado para operação {operation}")
            return
            
        departure_time = convert_timestamp(data.get('departure_time'))
        arrival_time = convert_timestamp(data.get('arrival_time'))
        
        row = [
            data.get('id'),
            data.get('flight_number'),
            data.get('origin'),
            data.get('destination'),
            departure_time,
            arrival_time,
            data.get('aircraft_type'),
            data.get('created_at'),
            datetime.now().isoformat(),
            operation
        ]
        
        with open(CSV_PATH, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(row)
        
        msg_fmt = f"Voo processado: {data.get('flight_number')} - {operation}"
        print(msg_fmt)
            
    except Exception as e:
        print(f"Erro ao processar mensagem: {e}")
        print("Conteúdo da mensagem:")
        print(msg.value().decode('utf-8') if msg.value() else 'None')


def main():
    try:
        ensure_csv_exists()
        
        consumer.subscribe([TOPIC])
        
        print(f"Iniciando consumo do tópico {TOPIC}...")
        print(f"Configurações: {config}")
        
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print("Chegou ao fim da partição")
                    continue
                else:
                    print(f"Erro: {msg.error()}")
                    break
                    
            process_message(msg)
            consumer.commit(msg)
            
    except KeyboardInterrupt:
        print("\nEncerrando consumidor...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()