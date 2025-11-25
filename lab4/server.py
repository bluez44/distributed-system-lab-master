import grpc
from concurrent import futures
import time
import json
import threading
import queue
from confluent_kafka import Producer, Consumer
import monitoring_pb2
import monitoring_pb2_grpc

# --- CẤU HÌNH ---
KAFKA_BOOTSTRAP_SERVER = 'kafka-0:9092' # IP:Port của Kafka (lưu ý port-forward)
SERVER_PORT = '[::]:50051'

# --- TRẠNG THÁI TOÀN CỤC (SHARED STATE) ---
# Danh bạ lưu các kết nối đang hoạt động
# Format: { "hostname": queue.Queue() }
active_clients = {}
clients_lock = threading.Lock()

# --- PHẦN 1: KAFKA PRODUCER (Gửi metrics đi) ---
# Cấu hình Producer
producer_conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVER}
kafka_producer = Producer(producer_conf)

def delivery_report(err, msg):
    """Callback để kiểm tra xem tin nhắn đã vào Kafka chưa"""
    if err is not None:
        print(f"[Kafka] Gửi lỗi: {err}")
    else:
        print(f"[Kafka] Đã gửi vào {msg.topic()} partition {msg.partition()}")

# --- PHẦN 2: KAFKA CONSUMER (Nhận lệnh về) ---
def run_kafka_consumer():
    """Luồng chạy ngầm để lắng nghe lệnh từ Analysis App"""
    consumer_conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVER,
        'group.id': 'server-command-group',
        'auto.offset.reset': 'latest' # Chỉ nhận lệnh mới
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe(['commands']) # Lắng nghe topic 'commands'

    print("[Kafka Consumer] Đang lắng nghe lệnh...")
    while True:
        msg = consumer.poll(1.0)
        if msg is None: continue
        if msg.error():
            print(f"[Kafka Consumer] Lỗi: {msg.error()}")
            continue

        try:
            # Format lệnh mong đợi: {"target": "Node-1", "action": "ALERT"}
            command_data = json.loads(msg.value().decode('utf-8'))
            target_host = command_data.get("target")
            action = command_data.get("action")
            
            print(f"[Kafka Consumer] Nhận lệnh cho {target_host}: {action}")

            # ĐỊNH TUYẾN: Tìm hàng đợi của đúng Agent đó
            with clients_lock:
                if target_host in active_clients:
                    # Thả lệnh vào hàng đợi của Agent
                    active_clients[target_host].put(action)
                    print(f"  -> Đã chuyển tiếp lệnh xuống gRPC stream của {target_host}")
                else:
                    print(f"  -> Agent {target_host} không online, bỏ qua lệnh.")
                    
        except Exception as e:
            print(f"[Kafka Consumer] Lỗi xử lý lệnh: {e}")

# --- PHẦN 3: gRPC SERVER (Cầu nối) ---
class MonitorService(monitoring_pb2_grpc.MonitorServiceServicer):
    
    def StreamMetrics(self, request_iterator, context):
        """
        Hàm này xử lý 1 kết nối gRPC từ 1 Agent.
        Nó chạy trong 1 luồng riêng biệt.
        """
        client_id = "Unknown"
        command_queue = queue.Queue() # Tạo hàng đợi riêng cho Client này

        try:
            # Vòng lặp nhận dữ liệu từ Agent (Upstream)
            for metric in request_iterator:
                
                # 1. Đăng ký vào danh bạ (nếu là gói tin đầu tiên)
                if client_id == "Unknown":
                    client_id = metric.hostname
                    with clients_lock:
                        active_clients[client_id] = command_queue
                    print(f"[gRPC] Agent '{client_id}' đã kết nối.")

                # 2. Chuyển tiếp Metrics sang Kafka (Produce)
                # Đóng gói thành JSON
                json_payload = json.dumps({
                    "timestamp": metric.timestamp,
                    "hostname": metric.hostname,
                    "metric": metric.metric,
                    "value": metric.value
                })
                
                print(f"[gRPC] Nhận metric từ {client_id}: {json_payload}")
                
                # Gửi vào Kafka topic 'metrics'
                kafka_producer.produce(
                    'metrics', 
                    json_payload.encode('utf-8'), 
                    callback=delivery_report
                )
                kafka_producer.poll(0) # Trigger callback ngay

                # 3. Kiểm tra xem có Lệnh nào cần gửi xuống không (Downstream)
                # Logic: Kiểm tra hàng đợi command_queue xem có gì không?
                # Lưu ý: check không chặn (non-blocking) để vòng lặp metrics không bị đứng
                while not command_queue.empty():
                    try:
                        cmd_action = command_queue.get_nowait()
                        print(f"[gRPC] Đang gửi lệnh '{cmd_action}' xuống {client_id}")
                        yield monitoring_pb2.CommandMessage(
                            command_id="CMD_Server", 
                            action=cmd_action
                        )
                    except queue.Empty:
                        break
                        
        except Exception as e:
            print(f"[gRPC] Lỗi kết nối với {client_id}: {e}")
        finally:
            # Dọn dẹp khi Client ngắt kết nối
            if client_id != "Unknown":
                with clients_lock:
                    if client_id in active_clients:
                        del active_clients[client_id]
                print(f"[gRPC] Agent '{client_id}' đã ngắt kết nối.")

def serve():
    # 1. Khởi động Kafka Consumer Thread
    consumer_thread = threading.Thread(target=run_kafka_consumer, daemon=True)
    consumer_thread.start()

    # 2. Khởi động gRPC Server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    monitoring_pb2_grpc.add_MonitorServiceServicer_to_server(MonitorService(), server)
    server.add_insecure_port(SERVER_PORT)
    
    print(f"Server Broker đang chạy tại {SERVER_PORT}...")
    server.start()
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()