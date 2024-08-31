import streamlit as st
from confluent_kafka import Consumer
import threading
import time
import json
from datetime import datetime,timedelta
from cassandra.cluster import Cluster
import requests
from PIL import Image
from io import BytesIO
import pandas as pd

# Configuration de Cassandra
CASSANDRA_HOST = 'cassandra'
CASSANDRA_PORT = 9042
CASSANDRA_KEYSPACE = 'weather_data'  # Remplace par ton keyspace

# Dictionnaire pour stocker les données Kafka en temps réel
kafka_data = {}

# Fonction pour consommer les données de Kafka et les stocker dans un dictionnaire
def consume_kafka_data(event):
    consumer_config = {
        'bootstrap.servers': 'kafka-projet-meteo:9092',
        'group.id': 'test',
        'auto.offset.reset': 'earliest'
    }
    
    consumer = Consumer(consumer_config)
    consumer.subscribe(['meteo_api_topic'])
    
    while not event.is_set():
        message = consumer.poll(0.2)
        
        if message is None:
            continue
        if message.error():
            print(f"Error: {message.error()}")
            continue
        
        record_key = message.key()
        record_value = message.value()
        data = json.loads(record_value)
        print("Message reçu : ", data)

        # Mettre à jour le dictionnaire avec les nouvelles données
        kafka_data[record_key] = data
        print("Data transférée \n")

def start_kafka_consumer():
    event = threading.Event()
    kafka_thread = threading.Thread(target=consume_kafka_data, args=(event,))
    kafka_thread.start()
    timer = threading.Timer(60.0, event.set)
    timer.start()

# Fonction pour récupérer les données historiques de Cassandra
def fetch_historical_data(ville):
    data = None
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    varConteur = 0
    while varConteur <= 10 : 
        try : 
            session = cluster.connect()
            session.set_keyspace(CASSANDRA_KEYSPACE)
            break
        except Exception as e:
            # Print the error and retry after a delay
            print(f"Erreur de connexion à Cassandra : {e}")
            print("Retrying in 5 seconds...")
            time.sleep(30) # Wait for 5 seconds before retrying
        finally:
            varConteur+=1
            
        
    # Calculer le timestamp il y a 12 heures
    twelve_hours_ago = datetime.now() - timedelta(hours=12)
    
    # Requête CQL pour obtenir les données des 12 dernières heures
    query = f"""
    SELECT * FROM weather_data_table
    WHERE Ville = '{ville}'
    AND Date >= '{twelve_hours_ago.strftime("%Y-%m-%d %H:%M:%S")}'
    ALLOW FILTERING;
    """
    rows = session.execute(query).all()
    
    if len(rows) > 0 :
        # Convertir les résultats en liste de dictionnaires
        data = [row._asdict() for row in rows] if rows else None
        #print("la data histo :",data,"\n")
        # Convertir le timestamp en format lisible si nécessaire
        for entry in data:
            #print("the entry : ", entry, "\n")
            if 'date' in entry:
                entry['date'] = entry['date'].strftime('%Y-%m-%d %H:%M:%S')
            

    return data

def get_weather_image(icon_data) : 
    try:
        image_url = f"https://openweathermap.org/img/wn/{icon_data}@4x.png"
        response = requests.get(image_url)
        response.raise_for_status()  # Lève une exception si la requête a échoué
        img = Image.open(BytesIO(response.content))
        resized_img = img.resize((100, 100))  # Width, Height
        return resized_img
    except requests.exceptions.RequestException as e:
        print(f"Erreur lors de la requête : {e}")
        return None
    except Exception as e:
        print(f"Erreur inattendue : {e}")
        return None

# Fonction d'affichage encapsulée
def display_data(co_1):
    # Liste des villes
    cities = ['Paris', 'Marseille', 'Rennes', 'Nice']

    #cities = kafka_data.keys()
    print("les villes :", cities,'\n')
    if cities :
        for i in range(0, len(cities), 2):
            cols = st.columns(2)  # Diviser l'affichage en 2 colonnes égales

            for j, city in enumerate(cities[i:i+2]):
                with cols[j]:
                    # Récupérer les données de Kafka pour cette ville
                    city_data = kafka_data.get(city.encode(), {})

                    if not city_data:
                        st.write(f"Aucune donnée disponible pour {city}")
                    else : 

                        # Extraire les valeurs actuelles des données
                        latest_temp = city_data.get('Temperature', 'N/A')
                        latest_date = city_data.get('Date', 'N/A')
                        latest_ville = city_data.get('Ville', 'N/A')
                        latest_feels_like = city_data.get('Feels_Like', 'N/A')
                        latest_min_temp = city_data.get('Min_Temperature', 'N/A')
                        latest_max_temp = city_data.get('Max_Temperature', 'N/A')
                        latest_humidity = city_data.get('Humidity', 'N/A')
                        latest_pressure = city_data.get('Pressure', 'N/A')
                        latest_wind_speed = city_data.get('Wind_Speed', 'N/A')
                        latest_wind_direction = city_data.get('Wind_Direction', 'N/A')
                        latest_weather = city_data.get('Weather_Descr', 'N/A')
                        icon = city_data.get('Icon', 'N/A')

                        # Obtenir l'URL de l'icône météo
                        weather_icon_image = get_weather_image(icon)

                        # Titre avec l'heure actuelle et icône météo
                        st.markdown(f"## 📍 {latest_ville} - {latest_date}")
                        st.image(weather_icon_image,use_column_width=False)

                        # Ajouter les métriques
                        col1, col2, col3 = st.columns(3)

                        with col1:
                            st.metric(label="🌡️ Température Actuelle", value=f"{latest_temp}°C")
                            st.metric(label="🌡️ Température Min", value=f"{latest_min_temp}°C")
                            st.metric(label="🌡️ Température Max", value=f"{latest_max_temp}°C")
                        with col2:
                            st.metric(label="💧 Humidité", value=f"{latest_humidity}%")
                            st.metric(label="🌬️ Vitesse du Vent", value=f"{latest_wind_speed} m/s")
                            st.metric(label="🧭 Direction du Vent", value=f"{latest_wind_direction}°")
                        with col3:
                            st.metric(label="🌡️ Temp. Ressentie", value=f"{latest_feels_like}°C")
                            st.metric(label="🔵 Pression", value=f"{latest_pressure} hPa")
                            st.metric(label="🌦️ Description", value=latest_weather)
                    #if (co_1 == 0 or co_1 == 4) : 
                        #co_1 = 0
                        # Afficher l'historique sous forme de tableau large
                    st.subheader(f"Historique des 30 derniers jours - {city}")
                    historical_data = fetch_historical_data(city)
                    if historical_data:
                        # Convertir les données historiques en DataFrame et afficher
                        df = pd.DataFrame(historical_data)
                        st.dataframe(df, use_container_width=True)
                    else:
                        st.write("Pas de données historiques disponibles")
                        #co_1 = co_1 + 1
                    #else : 
                        #co_1 = co_1 + 1
                    st.markdown("---")

# Fonction principale pour exécuter l'application Streamlit
def main():
    start_kafka_consumer()

    # PLUS DE LARGEUR 
    st.set_page_config(layout="wide")
    # Créer un conteneur vide pour les mises à jour des données
    placeholder = st.empty()
    co_1 = 0
    while True:
        with placeholder.container():
            st.title("Tableau de bord météo en temps réel")
            display_data(co_1)
        # Délai entre les mises à jour (en secondes)
        time.sleep(5)

if __name__ == "__main__":
    main()
