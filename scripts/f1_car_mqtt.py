import argparse
import sys
import csv
import time

import paho.mqtt.client as mqtt

def connectMQTT(broker_ip):
    """
    Connects to the MQTT broker and returns the client instance.
    :param broker_ip: IP del broker MQTT.
    """
    try:
        client = mqtt.Client()
        client.connect(broker_ip, 1883, 60)
        client.loop_start()
        print(f"Conectado al Broker MQTT con IP {broker_ip}")
        return client
    except Exception as e:
        print(f"No se pudo conectar al Broker MQTT en {broker_ip}: {e}")


def disconnectMQTT(client):
    """
    Desconecta del broker MQTT.
    :param client: Instancia del cliente MQTT.
    """
    try:
        if client.is_connected():
            client.loop_stop()
            client.disconnect()
            print("Desconectado del Broker MQTT.")
    except Exception as e:
        print(f"No se pudo desconectar del Broker MQTT: {e}")


def generate_telemetry_data(csv_file):
    """
    Lee datos de telemetría de F1 desde un archivo CSV y genera la carga útil y el tiempo de espera.
    Esta función es un generador.

    :param csv_file: Ruta al archivo CSV con datos de telemetría.
    Yields tuplas de (wait_time, distance, speed, throttle, brake, nGear, rpm).
    """
    try:
        with open(csv_file, 'r', newline='') as file:
            # Omitir la primera línea ("###")
            next(file)

            # Usar la segunda línea como encabezado del CSV
            reader = csv.DictReader(file, delimiter=';')
            print("Encabezado CSV:", reader.fieldnames)
            last_session_time = None

            for row in reader:
                try:
                    session_time = float(row['SessionTime_s'])

                    if last_session_time is None:
                        last_session_time = session_time

                    # Calcular el tiempo de espera antes de enviar el siguiente mensaje
                    wait_time = session_time - last_session_time
                    last_session_time = session_time

                    # Leer valores de telemetría
                    distance=float(row['Distance'])
                    speed=float(row['Speed'])   
                    throttle=float(row['Throttle'])
                    brake=float(row['Brake'])
                    nGear=int(row['nGear'])
                    rpm=float(row['RPM'])

                    yield wait_time, distance, speed, throttle, brake, nGear, rpm

                except (ValueError, KeyError) as e:
                    print(f"Omitiendo fila malformada o con datos faltantes: {row} - Error: {e}")
                    continue

    except FileNotFoundError:
        print(f"Error: No se encontró el archivo '{csv_file}'.")
    except Exception as e:
        print(f"Ocurrió un error inesperado: {e}")


def publish_telemetry(client, device_id, api_key, payload):
    """
    Publica con el topic /ul/<api_key>/<device_id>/attrs el contenido de "payload" al broker MQTT.

    :param client: Instancia del cliente MQTT.
    :param device_id: <device_id> del sensor dado de alta en el IoT Agent.
    :param api_key: Clave API del servicio al que pertenece el sensor.
    :param payload: Contenido del mensaje a publicar.
    """
    topic = f"/ul/{api_key}/{device_id}/attrs"
    client.publish(topic, payload)

def respond_cmd(client, userdata, msg):
    """
    Función callback para manejar comandos entrantes.
    Verifica el formato del topic y del payload y envía una respuesta.

    :param client: Instancia del cliente MQTT.
    :param userdata: Datos de usuario asociados al cliente (contiene device_id y api_key).
    :param msg: Mensaje MQTT recibido.
    """

    print(f"Comando recibido en tópico {msg.topic}: {msg.payload.decode()}")

    try:
        # Obtener device_id y api_key contenidos en userdata
        script_device_id = userdata['device_id']
        script_api_key = userdata['api_key']

        # Verificar formato y valores del topic
        expected_topic = f"/{script_api_key}/{script_device_id}/cmd"
        if msg.topic != expected_topic:
            print(f"Warning: Recibido un mensaje en un tópico inesperado: {msg.topic}")
            return

        # "parseo" del payload
        payload_str = msg.payload.decode()
        parts = payload_str.split('@drs|')
        if len(parts) != 2:
            print(f"Warning: Recibido payload mal formado: {payload_str}")
            return

        payload_device_id, value = parts
        if payload_device_id != script_device_id:
            print(f"Warning: El device_id ({payload_device_id}) del mensaje no corresponde con el device_id del tópico({script_device_id})")
            return

        # Genero respuesta en función del valor recibido
        response_payload = None
        if value == '1':
            response_payload = f"{script_device_id}@drs|ON"
        elif value == '0':
            response_payload = f"{script_device_id}@drs|OFF"
        else:
            print(f"Warning: Valor del comando recibido no válido: {value}")
            return

        # Publicar la confirmación de ejecución del comando
        response_topic = f"/ul/{script_api_key}/{script_device_id}/cmdexe"
        client.publish(response_topic, response_payload)
        print(f"Publicado respuesta en {response_topic}: {response_payload}")

    except Exception as e:
        print(f"Error procesando el comando: {e}")



def subscribe_to_cmd(client, device_id, api_key):
    """
    Se suscribe al tópico de comandos y configura el callback.

    :param client: Instancia del cliente MQTT.
    :param device_id: <device_id> del sensor dado de alta en el IoT Agent.
    :param api_key: Clave API del servicio al que pertenece el sensor.
    """

    # Guarda device_id y api_key para que el callback los use
    client.user_data_set({'device_id': device_id, 'api_key': api_key})

    # Configura la función callback para cuando se recibe un mensaje
    client.on_message = respond_cmd

    # Se suscribe al tópico de comandos
    topic = f"/{api_key}/{device_id}/cmd"
    client.subscribe(topic)
    print(f"Subscrito al tópico: {topic}")



if __name__ == "__main__":
    # Definición de argumentos de entrada del programa
    parser = argparse.ArgumentParser(description="Emulador de un coche de F1 que genera telemetrías")

    parser.add_argument(
        '--file',
        dest='csv_file',
        type=str,
        required=True,
        help='Ruta al archivo CSV con datos de telemetría.'
    )    
    parser.add_argument(
        '--mqtt',
        dest='mqtt_broker',
        type=str,
        required=True,
        help='IP del broker MQTT.'
    )
    parser.add_argument(
        '--device',
        dest='device_id',
        type=str,
        required=True,
        help='<device_id> del dispositivo dado de alta en el IoT Agent.'
    )
    parser.add_argument(
        '--apikey',
        dest='api_key',
        type=str,
        required=True,
        help='<api_key> del servicio al que pertenece el sensor.'
    )

    # Leer los argumentos de entrada
    args = parser.parse_args()


    try:
        # Conecta con el broker MQTT
        clientmqtt=connectMQTT(args.mqtt_broker)

        # Suscribirse al tópico de comandos
        subscribe_to_cmd(clientmqtt, args.device_id, args.api_key)

        # Leer y publicar datos de telemetría
        t0 = time.monotonic()
    
        for wait_time, distance, speed, throttle, brake, nGear, rpm in generate_telemetry_data(args.csv_file):
            if wait_time > 0: # Espero el tiempo necesario antes de enviar el siguiente mensaje
                time.sleep(wait_time)
            
            # Payload del mensaje MQTT
            payload = f'd|{distance}|s|{speed}|t|{throttle}|b|{brake}|g|{nGear}|r|{rpm}'

            elapsed_time = time.monotonic() - t0
            print(f"[{elapsed_time:.2f}s] Published: {payload}")

            # Publicar datos de telemetría
            publish_telemetry(clientmqtt, args.device_id, args.api_key, payload)
            
            
    except Exception as e:
        print(f"Ocurrió un error: {e}")
    finally:
        if 'clientmqtt' in locals():
            disconnectMQTT(clientmqtt)
    