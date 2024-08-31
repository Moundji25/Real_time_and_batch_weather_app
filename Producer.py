from confluent_kafka.admin import AdminClient, NewTopic
import datetime
import pytz
from cassandra.cluster import Cluster
from confluent_kafka import Producer
import socket
import json
import time
import requests 
import pandas as pd
from time import sleep


def init_kafka (): 
    a = AdminClient({'bootstrap.servers': 'kafka-projet-meteo:9092','debug': 'broker,admin'})
    
    new_topics = [NewTopic('meteo_api_topic', num_partitions=3, replication_factor=1)]
    # Note : Dans un scénario de production multi-clusters, il est plus habituel d'utiliser un facteur de réplication de 3 pour la durabilité.

    fs = None
    # Appelez create_topics pour créer des sujets de manière asynchrone. Un dict # de <topic,future> est renvoyé.
    if topic_exists(a, 'meteo_api_topic') == False : 
        fs = a.create_topics(new_topics)

    if fs is None : 
        print("ERREUR CREATION TOPIC \n")
    else : 
        # Attendre la fin de chaque opération.
        for topic, f in fs.items():
            try:
                f.result()  # Le résultat en lui-même est None
                print("Topic {} created".format(topic))
            except Exception as e:
                print(e)
                print("Failed to create topic {}: {}".format(topic, e))
            
def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result.
    Triggered by poll() or flush()."""
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


def topic_exists(client, topic_name):
    """Checks if the given topic exists"""
    topic_metadata = client.list_topics(timeout=5)
    for t in topic_metadata.topics.values():
        if t.topic == topic_name:
            return True
    return False

def list_all_topics(client):
    """Lists all the available topics"""
    topic_metadata = client.list_topics(timeout=5)
    for t in topic_metadata.topics.values():
        print(t.topic)
        
def call_to_api(api_key, lat, lon):
    api_key = "d8d38e83519e945d12c19de96142dc64"
   
    base_url = "http://api.openweathermap.org/data/2.5/weather?"
    complete_url = f"{base_url}lat={lat}&lon={lon}&appid={api_key}"
    #complete_url = "https://api.openweathermap.org/data/2.5/weather?lat=39.197498&lon=44.26556&appid=d8d38e83519e945d12c19de96142dc64"
     
    try:
        response = requests.get(complete_url)
        response.raise_for_status()  # Raise an exception for 4xx or 5xx status codes
    except requests.exceptions.RequestException as e:
        print(f"Error making API request: {e}")
        return None

    data = response.json()
     
    if data["cod"] == 200:
        # Extract relevant data
        main = data["main"]
        weather = data["weather"][0]
        wind = data["wind"]
         
        weather_data = {
            1: {
                "Temperature": round((main["temp"] - 273.15),2),
                "Date": datetime.datetime.fromtimestamp(data["dt"], pytz.timezone('Europe/Paris')).strftime("%Y-%m-%d %H:%M:%S"),
                "Ville": data["name"],
                "Feels_Like": round((main["feels_like"] - 273.15),2),
                "Min_Temperature": round((main["temp_min"] - 273.15),2),
                "Max_Temperature": round((main["temp_max"] - 273.15),2),
                "Pressure": main["pressure"],
                "Humidity": main["humidity"],
                "Weather_Descr": weather["description"],
                "Wind_Speed": wind["speed"],
                "Wind_Direction": wind.get("deg", "N/A"),
                "Icon": weather["icon"]
            }
        }
     
        return weather_data
    else :
        print(f"City not found or error in API request, error code: {data['cod']}")

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
            time.sleep(30)  # Wait for 5 seconds before retrying
        finally:
            varConteur+=1

def main():
    conf = {
        'bootstrap.servers': "PLAINTEXT://kafka-projet-meteo:9092,PLAINTEXT://localhost:9092",
        'client.id': socket.gethostname()
    }

    API_KEY = 'd8d38e83519e945d12c19de96142dc64'
    # Example usage
    les_villes = les_villes = [{"id": 2988507, "name": "Paris", "state": "", "country": "FR", "coord": {"lon": 2.3488, "lat": 48.853409}}, {"id": 2990440, "name": "Nice", "state": "", "country": "FR", "coord": {"lon": 7.26608, "lat": 43.703129}}, {"id": 2792360, "name": "Lille", "state": "", "country": "BE", "coord": {"lon": 4.82312, "lat": 51.24197}}, {"id": 2995469, "name": "Marseille", "state": "", "country": "FR", "coord": {"lon": 5.38107, "lat": 43.296951}}, {"id": 2983990, "name": "Rennes", "state": "", "country": "FR", "coord": {"lon": -1.67429, "lat": 48.11198}}]
    while True : 
        for city in les_villes :
        
            lon = city['coord']["lon"]
            lat = city['coord']["lat"]
            
            weather_data = call_to_api(API_KEY,lat,lon)
            
            if weather_data is None : 
                print("ERREUR REQUETE API")
                exit(1)
            else :
            
                print('LE RESULTAT DE l\'API : \n')
                for key, value in weather_data.items():
                    for key_1, value_1 in value.items() : 
                        print(key_1," : ",value_1, " \n")
            
                #partie cassandra
                connect_to_cassandra()

                # PARTIE KAFKA 
                init_kafka()
                p = Producer(conf)
                
                # Initialiser le nom du topic
                topic_name = 'meteo_api_topic'
                
                # Parcourir comme key-value et generer un ecrire les données (keyn value) avec
                for key, value in weather_data.items():
                    # Lancer un rappel de rapport de livraison disponible à partir d'appels de
                    # production () précédents
                    p.poll(0)
                    # Produire de manière synchrone un message, les callbacks
                    # seront déclenchés à partir de poll() au-dessus, ou de flush() au-dessous,
                    # lorsque le message a
                    # ont été livrés avec succès ou ont échoué définitivement.
                    data = json.dumps(value, ensure_ascii=False).encode('utf-8')
                
                    p.produce(topic_name, value=data, key=str(value['Ville']), callback=delivery_report)
                
                # Attendre la remise des messages en attente et du rapport de remise
                # les callbacks à déclencher.
                p.flush()
        sleep(1)

if __name__ == "__main__":
    main()
