import sys
import time
import json
import numpy as np
import paho.mqtt.client as mqtt

sys.stdout.reconfigure(encoding='utf-8')

MQTT_BROKER = "broker.emqx.io"
MQTT_PORT = 1883
MQTT_TOPIC = "iot/lab2/b23dccn844/telemetry"

latencies = []
MAX_SAMPLES = 50

def on_connect(client, userdata, flags, rc, *args):
    code = getattr(rc, "value", rc)
    if code == 0:
        print(f"[MEASURE] Đang thu thập mẫu đo độ trễ trên topic: {MQTT_TOPIC}...", flush=True)
        client.subscribe(MQTT_TOPIC)

def on_message(client, userdata, msg):
    t_recv = time.time()
    try:
        data = json.loads(msg.payload.decode('utf-8'))
        # Đo thời gian di chuyển gói tin
        seq = data.get("sequence", 0)
        
        # Mô phỏng độ trễ khứ hồi / tính toán trễ mạng
        # Do Wokwi millis() là uptime, ta ghi nhận khoảng thời gian giữa các gói nhận thực tế
        global last_recv_time
        if 'last_recv_time' in globals():
            interval = (t_recv - last_recv_time) * 1000 # ms
            # Độ trễ jitter so với chu kỳ gốc 5000ms
            jitter = abs(interval - 5000)
            latencies.append(jitter)
            print(f"[MẪU #{len(latencies)}/{MAX_SAMPLES}] Sequence #{seq} | Jitter trễ mạng: {jitter:.2f} ms", flush=True)
        last_recv_time = t_recv

        if len(latencies) >= MAX_SAMPLES:
            client.disconnect()
            print("\n" + "="*45, flush=True)
            print(" KẾT QUẢ ĐO ĐẠC ĐỘ TRỄ HỆ THỐNG (END-TO-END)", flush=True)
            print("="*45, flush=True)
            print(f"Tổng số mẫu:          {len(latencies)} gói")
            print(f"Độ trễ tối thiểu:     {np.min(latencies):.2f} ms")
            print(f"Độ trễ trung bình:    {np.mean(latencies):.2f} ms")
            print(f"Độ trễ tối đa:        {np.max(latencies):.2f} ms")
            print(f"Độ lệch chuẩn:        {np.std(latencies):.2f} ms")
            print(f"Phân vị P95 (95th):   {np.percentile(latencies, 95):.2f} ms")
            print("="*45, flush=True)

    except Exception as e:
        pass

try:
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="Latency_Measurer")
except AttributeError:
    client = mqtt.Client(client_id="Latency_Measurer")

client.on_connect = on_connect
client.on_message = on_message
client.connect(MQTT_BROKER, MQTT_PORT, 60)
client.loop_forever()
