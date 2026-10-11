import sys
import json
import time
import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# 1. Ep buoc Terminal Windows xuat tieng Viet theo bang ma UTF-8
sys.stdout.reconfigure(encoding='utf-8')

# 2. Cau hinh MQTT
MQTT_BROKER = "broker.emqx.io"
MQTT_PORT = 1883
MQTT_TOPIC = "iot/lab2/b23dccn844/telemetry"

# 3. Cau hinh InfluxDB 2.x
INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "my-super-secret-auth-token"
INFLUX_ORG = "iot_lab"
INFLUX_BUCKET = "iot_raw"

db_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = db_client.write_api(write_options=SYNCHRONOUS)

def validate_data(data):
    """Kiem tra tinh toan ven va pham vi vat ly cua cam bien"""
    required = ["temperature", "humidity", "distance_cm", "sequence", "uptime_s"]
    if not all(k in data for k in required):
        return False
    if not (-40.0 <= data["temperature"] <= 125.0):
        return False
    if not (0.0 <= data["humidity"] <= 100.0):
        return False
    if not (2.0 <= data["distance_cm"] <= 450.0):
        return False
    return True

# Tuong thich ca paho-mqtt v1 va v2
def on_connect(client, userdata, flags, rc, *args):
    # Doi voi paho-mqtt v2, rc la mot doi tuong ReasonCode co gia tri .value == 0
    code = getattr(rc, "value", rc)
    if code == 0:
        print("[MQTT] Ket noi Broker EMQX thanh cong!")
        client.subscribe(MQTT_TOPIC)
        print(f"[MQTT] Dang lang nghe topic: {MQTT_TOPIC}")
    else:
        print(f"[MQTT] Ket noi that bai, ma loi: {code}")

def on_message(client, userdata, msg):
    try:
        payload_str = msg.payload.decode('utf-8')
        data = json.loads(payload_str)

        if not validate_data(data):
            print(f"[REJECTED] Du lieu sai dinh dang hoac vuot nguong: {payload_str}")
            return

        point = (
            Point("sensor_telemetry")
            .tag("device_id", "esp32_wokwi")
            .tag("sensor_group", "env_ultrasonic")
            .field("temperature", float(data["temperature"]))
            .field("humidity", float(data["humidity"]))
            .field("distance_cm", float(data["distance_cm"]))
            .field("rssi", int(data.get("rssi", 0)))
            .field("sequence", int(data["sequence"]))
            .field("uptime_s", int(data["uptime_s"]))
            .time(time.time_ns(), WritePrecision.NS)
        )
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
        print(f"[STORED RAW #{data['sequence']}] T={data['temperature']}°C | H={data['humidity']}% | Dist={data['distance_cm']}cm")

    except Exception as e:
        print(f"[ERROR]: {e}")

# Khoi tao MQTT Client ho tro ca paho-mqtt v2.x va v1.x
try:
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="B23DCCN844_Ingest_Worker")
except AttributeError:
    mqtt_client = mqtt.Client(client_id="B23DCCN844_Ingest_Worker")

mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

print("[SYSTEM] Khoi dong Ingestion Worker...")
mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
mqtt_client.loop_forever()
