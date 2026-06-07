import paho.mqtt.client as mqtt
import json
import time
import random

BROKER = "broker.hivemq.com" # Broker público para pruebas
PORT = 1883
TOPIC = "fisi/smat/estaciones/1"

client = mqtt.Client()
client.connect(BROKER, PORT)

print("🚀 Sensor MQTT iniciado")
print(f"📡 Publicando en: {TOPIC}")

try:
    while True:

        payload = {
            "valor": round(random.uniform(20.0, 60.0), 2),
            "timestamp": time.time()
        }

        resultado = client.publish(TOPIC,json.dumps(payload))

        # Verificar si la publicación fue exitosa
        if resultado.rc == mqtt.MQTT_ERR_SUCCESS:
            print(f"📤 Enviado por MQTT: {payload}")
        else:
            print(
                f"⚠️ Error al publicar mensaje "
                f"(código {resultado.rc})"
            )

        time.sleep(10)

except KeyboardInterrupt:
    print("\n🛑 Deteniendo sensor...")

finally:
    client.disconnect()
    print("✅ Desconectado del broker")