import mysql.connector
import time
from datetime import datetime, timedelta


def create_connection():
    time.sleep(10)
    
    return mysql.connector.connect(
        host="mysql",
        user="root",
        password="debezium",
        database="flights"
    )


def create_flights_table(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS flights (
            id INT AUTO_INCREMENT PRIMARY KEY,
            flight_number VARCHAR(10) NOT NULL,
            origin VARCHAR(3) NOT NULL,
            destination VARCHAR(3) NOT NULL,
            departure_time DATETIME NOT NULL,
            arrival_time DATETIME NOT NULL,
            aircraft_type VARCHAR(20),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)


def insert_sample_flights(cursor):
    sample_flights = [
        ("LA2451", "GRU", "MIA", "2024-03-20 10:00:00", "2024-03-20 18:30:00", "Boeing 787"),
        ("AA1234", "JFK", "LAX", "2024-03-20 08:15:00", "2024-03-20 11:45:00", "Airbus A321"),
        ("G31234", "GRU", "GIG", "2024-03-20 09:30:00", "2024-03-20 10:30:00", "Airbus A320"),
        ("AZ1023", "GRU", "FCO", "2024-03-20 22:00:00", "2024-03-21 14:00:00", "Boeing 777"),
    ]

    insert_query = """
        INSERT INTO flights 
        (flight_number, origin, destination, departure_time, arrival_time, aircraft_type)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    
    cursor.executemany(insert_query, sample_flights)


def main():
    try:
        conn = create_connection()
        cursor = conn.cursor()

        create_flights_table(cursor)
        
        while True:
            insert_sample_flights(cursor)
            
            conn.commit()
            print("Dados inseridos com sucesso!")
            
            time.sleep(15)

    except mysql.connector.Error as err:
        print(f"Erro: {err}")
    
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()
            print("Conexão com o banco de dados fechada.")


if __name__ == "__main__":
    main() 