import paho.mqtt.client as mqtt
import requests
import json
import threading
import time

# CONFIGURACIÓN
BROKER = "broker.hivemq.com"
TOPIC = "fisi/smat/estaciones/#"
API_URL = "http://localhost:8000/lecturas/"

# Configuración para obtener el token de autenticación
LOGIN_URL = "http://localhost:8000/token" 
form_data = {"username": "admin", "password": "12345"}  
token_recibido = requests.post(LOGIN_URL, data=form_data)
TOKEN = token_recibido.json().get("access_token")

# Registro del último mensaje recibido por estación
last_seen = {}


def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        print(f"📩 Mensaje recibido en {msg.topic}: {payload}")

        estacion_id = msg.topic.split('/')[-1]

        # Actualizar última vez vista
        last_seen[estacion_id] = time.time()

        data_to_send = {
            "estacion_id": int(estacion_id),
            "valor": payload["valor"],
        }

        headers = {
            "Authorization": f"Bearer {TOKEN}"
        }
        try:
            response = requests.post(
                API_URL,
                json=data_to_send,
                headers=headers
            )

            if response.status_code == 201:
                print(f"✅ Dato persistido en DB para estación {estacion_id}")
            else:
                print(
                    f"⚠️ Error API ({response.status_code}): "
                    f"{response.text}"
                )
        except requests.exceptions.ConnectionError:
            print("⚠️ La API no está disponible")

        except requests.exceptions.Timeout:
            print("⚠️ Tiempo de espera agotado al conectar con la API")

    except Exception as e:
        print(f"❌ Error procesando mensaje: {e}")


def check_deadlines():
    while True:
        current_time = time.time()

        for eid, last_time in list(last_seen.items()):
            if current_time - last_time > 30:
                print(f"🚨 ALERTA: Estación {eid} está OFFLINE")

        time.sleep(10)


# Iniciar hilo de monitoreo
threading.Thread(
    target=check_deadlines,
    daemon=True
).start()

# Cliente MQTT
client = mqtt.Client()
client.on_message = on_message

print("🚀 Bridge SMAT iniciado. Esperando datos...")

client.connect(BROKER, 1883)
client.subscribe(TOPIC)

try:
    client.loop_forever()

except KeyboardInterrupt:
    print("\n🛑 Cerrando Bridge MQTT...")

finally:
    client.disconnect()
    print("✅ Desconectado del broker")