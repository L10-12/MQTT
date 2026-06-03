import json
import time
import paho.mqtt.client as mqtt
from pydantic import BaseModel, Field, ValidationError

# definimos el esquema con sus limites de validación para las lecturas de temperatura
class LecturaSensor(BaseModel):
    sensor_id: int
    timestamp: float
    valor: float = Field(..., ge=-50.0, le=100.0) 
    unidad: str

BROKER = "broker.hivemq.com"
PUERTO = 1883
TOPICO_WILDCARD = "unmsm/callao/camara/+/telemetria"
UMBRAL_PELIGRO = 5.0

def registrar_error_log(topico, raw_payload, error_msg):
    """Guarda los datos corruptos o inválidos en un archivo log_errores.txt"""
    timestamp_actual = time.strftime("%Y-%m-%d %H:%M:%S")
    with open("log_errores.txt", "a", encoding="utf-8") as f:
        f.write(f"[{timestamp_actual}] TOPICO: {topico}\n")
        f.write(f"PAYLOAD: {raw_payload}\n")
        f.write(f"ERROR: {error_msg}\n")
        f.write("-" * 60 + "\n")

def on_connect(client, userdata, flags, rc, properties):
    if rc == 0:
        print("Conectado exitosamente al Broker MQTT")
        client.subscribe(TOPICO_WILDCARD)
        print(f"Suscrito al tópico dinámico: {TOPICO_WILDCARD}")
    else:
        print(f"Error de conexión: {rc}")

def on_message(client, userdata, msg):
    raw_payload = msg.payload.decode()
    
    # aqui se extrae el id de la cámara directamente desde el tópico recibido
    partes_topico = msg.topic.split('/')
    id_camara = partes_topico[3] if len(partes_topico) >= 4 else "Desconocida"

    try:
        datos_json = json.loads(raw_payload)
        #aqui validamos con pydantic
        lectura = LecturaSensor(**datos_json)
        # si pasa la validación verificamos el umbral de temperatura de la empresa
        if lectura.valor > UMBRAL_PELIGRO:
            print(f"\n[PELIGRO] Pérdida de cadena de frío en {id_camara.upper()}! Temperatura actual: {lectura.valor}°C")
        else:
            print(f"\n[OK] {id_camara.upper()}: {lectura.valor}°C (Cadena de frío segura)")

    except json.JSONDecodeError as e:
        print(f"\n[ERROR] JSON inválido detectado en {id_camara}. Registrando en log...")
        registrar_error_log(msg.topic, raw_payload, f"JSONDecodeError: {str(e)}")
        
    except ValidationError as e:
        print(f"\n[ALERTA DE SEGURIDAD] Violación de integridad en {id_camara}. Registrando en log...")
        registrar_error_log(msg.topic, raw_payload, f"PydanticValidationError:\n{str(e)}")

def main():
    cliente = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    cliente.on_connect = on_connect
    cliente.on_message = on_message
    
    cliente.connect(BROKER, PUERTO, 60)
    cliente.loop_forever()

if __name__ == "__main__":
    main()