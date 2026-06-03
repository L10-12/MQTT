import json
import paho.mqtt.client as mqtt
from pydantic import BaseModel, Field, ValidationError

# Definimos el esquema de datos esperado usando Pydantic
class LecturaSensor(BaseModel):
    sensor_id: int
    timestamp: float
    valor: float = Field(..., ge=-50.0, le=100.0) # Validación de límites físicos de temperatura
    unidad: str

BROKER = "broker.hivemq.com"
PUERTO = 1883
TOPICO = "unmsm/fisi/cc/sensor/temperatura"

# Callback cuando el cliente recibe una confirmación de conexión (CONNACK) del broker
def on_connect(client, userdata, flags, rc, properties):
    if rc == 0:
        print("Conectado exitosamente al Broker MQTT")
        # Suscribirse al tópico de interés
        client.subscribe(TOPICO)
        print(f"Suscrito a: {TOPICO}")
    else:
        print(f"Error de conexión. Código de retorno: {rc}")

# Callback cuando llega un mensaje publicado al tópico suscrito
def on_message(client, userdata, msg):
    raw_payload = msg.payload.decode()
    print(f"\n[SUBSCRIBER] Mensaje recibido en {msg.topic}")
    
    try:
        # Intentar transformar de JSON plano a Objeto Validado
        datos_json = json.loads(raw_payload)
        lectura = LecturaSensor(**datos_json)
        
        # Procesar lectura validada de forma segura
        print(f"-> Datos Validados Correctamente. ID: {lectura.sensor_id}")
        print(f"-> Temperatura Registrada: {lectura.valor} {lectura.unidad}")
        
    except json.JSONDecodeError:
        print("[ALERTA] Los datos recibidos no corresponden a un formato JSON válido.")
    except ValidationError as e:
        print(f"[ALERTA DE SEGURIDAD] Violación de integridad de datos:\n{e}")

def main():
    cliente = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    
    # Asignar los callbacks de eventos de red
    cliente.on_connect = on_connect
    cliente.on_message = on_message
    
    cliente.connect(BROKER, PUERTO, 60)
    
    # Iniciar bucle síncrono infinito para escuchar mensajes de red
    cliente.loop_forever()

if __name__ == "__main__":
    main()
