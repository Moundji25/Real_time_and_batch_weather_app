from confluent_kafka import Consumer
import json
from cassandra.cluster import Cluster
import os
import time
from datetime import datetime, timedelta

def connect_to_cassandra():
    # Cassandra configuration
    CASSANDRA_HOST = 'cassandra'
    CASSANDRA_PORT = '9042'
    CASSANDRA_KEYSPACE = "weather_data"
    varConteur = 0

    # Infinite loop to retry connection until successful
    while varConteur<=10:
        try:
            # Attempt to connect to Cassandra
            cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
            session = cluster.connect()
            
            # Create keyspace if it doesn't exist
            session.execute("""
                CREATE KEYSPACE IF NOT EXISTS weather_data 
                WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
            """)
            session.set_keyspace(CASSANDRA_KEYSPACE)
            
            # Create table if it doesn't exist
            session.execute("""
                CREATE TABLE IF NOT EXISTS weather_data_table (
                    Temperature FLOAT,
                    Date TIMESTAMP,
                    Ville TEXT,
                    Feels_Like FLOAT,
                    Min_Temperature FLOAT,
                    Max_Temperature FLOAT,
                    Pressure INT,
                    Humidity INT,
                    Weather_Descr TEXT,
                    Wind_Speed FLOAT,
                    Wind_Direction INT,
                    PRIMARY KEY (Date, Ville)
                );
            """)
            
            print("Connected to Cassandra successfully.")
            return session

        except Exception as e:
            # Print the error and retry after a delay
            print(f"Erreur de connexion à Cassandra : {e}")
            print("Retrying in 5 seconds...")
            time.sleep(30) # Wait for 5 seconds before retrying
        finally:
            varConteur+=1
def main():
    conf = {
        "bootstrap.servers": 'kafka-projet-meteo:9092',
        "group.id": 0
    }

    topic_name = 'meteo_api_topic'
    CASSANDRA_KEYSPACE = "weather_data"
    consumer = Consumer(conf)

    # Subscribe to topic
    consumer.subscribe([topic_name])

    # Initialize Cassandra session
    session = connect_to_cassandra()

    # Batch data buffer and timing
    buffer = []
    batch_interval = timedelta(minutes=2)  # 10 minutes interval
    last_batch_time = datetime.now()

    try:
        while True:
            msg = consumer.poll(0.01)

            if msg is None:
                # No message available within timeout.
                continue

            elif msg.error():
                print('Error: {}'.format(msg.error()))

            else:
                # Check for Kafka message
                record_value = msg.value()
                data = json.loads(record_value)
                print('message recu >>>>>>>>>>>>>>>>>>>>>>> ', data,'\n\n')
                # Append data to buffer
                buffer.append(data)

                # Check if it's time to save the batch
                current_time = datetime.now()
                # Using f-strings to format the output
                print(f"{current_time} - - {current_time - last_batch_time}")

                if current_time - last_batch_time >= batch_interval:
                    # Insert buffered data into Cassandra
                    for data in buffer:
                        temperature = data.get('Temperature')
                        feels_like = data.get('Feels_Like')
                        min_temperature = data.get('Min_Temperature')
                        max_temperature = data.get('Max_Temperature')
                        pressure = data.get('Pressure')
                        humidity = data.get('Humidity')
                        weather_description = data.get('Weather_Descr')
                        wind_speed = data.get('Wind_Speed')
                        wind_direction = data.get('Wind_Direction')
                        city = data.get('Ville')
                        timestamp = data.get('Date') 

                        # Insert data into Cassandra
                        session.execute("""
                            INSERT INTO weather_data_table (Temperature, Date, Ville, Feels_Like, Min_Temperature, Max_Temperature, 
                            Pressure, Humidity, Weather_Descr, Wind_Speed, Wind_Direction)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (temperature, timestamp, city, feels_like, min_temperature, max_temperature, pressure, humidity, 
                            weather_description, wind_speed, wind_direction))
                    
                    # Clear the buffer and update the last batch time
                    buffer.clear()
                    last_batch_time = current_time
                    print("Stored batch of data in Cassandra")

    except KeyboardInterrupt:
        print("Interrupted by user")

    finally:
        # Close Kafka consumer
        consumer.close()
        # Optional: Close Cassandra session if needed
        if session:
            session.shutdown()

if __name__ == "__main__":
    main()
