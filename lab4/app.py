import gradio as gr
import etcd3
import json
import threading
import time
from confluent_kafka import Consumer, Producer

# --- CẤU HÌNH KẾT NỐI (LƯU Ý PORT-FORWARD) ---
ETCD_HOST = 'etcd-0'
ETCD_PORT = 2379
KAFKA_BOOTSTRAP = 'kafka-0:9092'

# --- BIẾN TOÀN CỤC ---
# Lưu dữ liệu mới nhất của các node để hiển thị lên Dashboard
# Format: {'node1': {'time': '...', 'cpu': 10, 'ram': 20}}
node_data_store = {} 

# --- KẾT NỐI HẠ TẦNG ---
try:
    etcd_client = etcd3.client(host=ETCD_HOST, port=ETCD_PORT)
    kafka_producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP})
except Exception as e:
    print(f"Lỗi kết nối hạ tầng: {e}")

# --- PHẦN 1: BACKGROUND WORKER (KAFKA CONSUMER) ---
def run_kafka_consumer():
    """Luồng ngầm đọc data từ Kafka để cập nhật Dashboard"""
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'group.id': 'gradio-dashboard-group',
        'auto.offset.reset': 'latest'
    }
    consumer = Consumer(conf)
    consumer.subscribe(['metrics'])

    while True:
        msg = consumer.poll(1.0)
        if msg is None: continue
        if msg.error():
            print(f"Kafka Error: {msg.error()}")
            continue
        
        try:
            # Parse dữ liệu: {hostname, metric, value, timestamp}
            data = json.loads(msg.value().decode('utf-8'))
            host = data.get('hostname')
            metric = data.get('metric')
            val = data.get('value')
            ts = data.get('timestamp')

            # Cập nhật vào kho dữ liệu chung
            if host not in node_data_store:
                node_data_store[host] = {'Hostname': host}
            
            node_data_store[host][metric] = val
            node_data_store[host]['Last Update'] = ts
            
        except Exception as e:
            print(f"Error parsing msg: {e}")

# Khởi động luồng Consumer ngay khi chạy app
threading.Thread(target=run_kafka_consumer, daemon=True).start()

# --- PHẦN 2: CÁC HÀM XỬ LÝ LOGIC (BACKEND) ---

def get_active_nodes():
    """Quét etcd heartbeat để tìm các node đang online"""
    try:
        active_nodes = []
        # Lấy tất cả key bắt đầu bằng /monitor/heartbeat/
        result = etcd_client.get_prefix("/monitor/heartbeat/")
        
        for value, meta in result:
            key = meta.key.decode('utf-8')
            # Key format: /monitor/heartbeat/<hostname>
            node_name = key.split('/')[-1]
            active_nodes.append(node_name)
        
        # Loại bỏ trùng lặp
        unique_nodes = list(set(active_nodes))
        
        if not unique_nodes:
            # Nếu không tìm thấy node nào, trả về danh sách rỗng và reset giá trị
            return gr.Dropdown(choices=[], value=None, label="Không tìm thấy Node nào")
        
        # NẾU TÌM THẤY:
        # Cập nhật choices bằng danh sách node mới
        # Cập nhật value bằng node đầu tiên trong danh sách (để người dùng đỡ phải chọn)
        return gr.Dropdown(choices=unique_nodes, value=unique_nodes[0], label="Chọn Node để cấu hình")
        
    except Exception as e:
        # Trường hợp lỗi thì thông báo vào label
        return gr.Dropdown(choices=[], value=None, label=f"Lỗi etcd: {str(e)}")

def update_dashboard():
    """Chuyển đổi dữ liệu từ dict sang list để hiển thị lên bảng Gradio"""
    # Chuyển dictionary thành list of lists cho Dataframe
    # Cấu trúc: [Hostname, CPU, Memory, Last Update]
    data_list = []
    for host, info in node_data_store.items():
        cpu = info.get('cpu', 'N/A')
        mem = info.get('memory', 'N/A')
        diskRead = info.get('disk read', 'N/A')
        diskWrite = info.get('disk write', 'N/A')
        netIn = info.get('net in', 'N/A')
        netOut = info.get('net out', 'N/A')
        ts = info.get('Last Update', 'N/A')
        
        # Làm tròn số nếu có
        if isinstance(cpu, float): cpu = round(cpu, 2)
        if isinstance(mem, float): mem = round(mem, 2)
        
        data_list.append([host, cpu, mem, diskRead, diskWrite, netIn, netOut, ts])
    
    return data_list

def push_config(node_name, interval, metrics, plugins):
    """Đẩy cấu hình xuống etcd"""
    if not node_name or node_name == "Chưa tìm thấy Node nào":
        return "⚠️ Vui lòng chọn một Node hợp lệ!"
    try:
        plugins_list = []
        for plugin in plugins:
            plugins_list.append("plugins." + plugin + "." + plugin[0].upper() + plugin[1:] + "Plugin")
        
        config_payload = {
            "interval": float(interval),
            "metrics": metrics,
            "plugins": plugins_list
        }
        
        key = f"/monitor/config/{node_name}"
        etcd_client.put(key, json.dumps(config_payload))
        return f"✅ Thành công: Đã cập nhật cho {node_name}!\nConfig: {json.dumps(config_payload)}"
    except Exception as e:
        return f"❌ Lỗi: {str(e)}"

def send_command(node_name, command_text):
    """Gửi lệnh xuống Kafka"""
    if not node_name or node_name == "Chưa tìm thấy Node nào":
        return "⚠️ Vui lòng chọn một Node hợp lệ!"
    
    try:
        payload = json.dumps({
            "target": node_name,
            "action": command_text
        })
        kafka_producer.produce('commands', payload.encode('utf-8'))
        kafka_producer.flush()
        return f"🚀 Đã gửi lệnh '{command_text}' tới {node_name}"
    except Exception as e:
        return f"❌ Lỗi Kafka: {str(e)}"

# --- PHẦN 3: GIAO DIỆN GRADIO (FRONTEND) ---

with gr.Blocks(title="Distributed Monitor Admin") as demo:
    gr.Markdown("# 🚀 Hệ Thống Giám Sát Phân Tán (Admin Dashboard)")
    
    # === TAB 1: DASHBOARD (STREAMING MODE) ===
    with gr.Tab("📊 Live Dashboard"):
        gr.Markdown("Dữ liệu được cập nhật Real-time (Streaming).")
        output_table = gr.Dataframe(
            headers=["Hostname", "CPU (%)", "Memory (%)", "Disk Read", "Disk Write", "Net In", "Net Out", "Last Timestamp"],
            datatype=["str", "number", "number", "number", "number", "number", "number", "str"],
            interactive=False
        )

        # Hàm Generator: Chạy liên tục và "nhả" (yield) dữ liệu ra UI
        def stream_dashboard():
            while True:
                # Gọi hàm lấy data (đã viết ở phần Backend cũ)
                data = update_dashboard() 
                yield data
                time.sleep(0.1) # Nghỉ cực ngắn để không treo máy

        # Kích hoạt chế độ stream ngay khi Tab được load
        demo.load(stream_dashboard, outputs=output_table)

    # === TAB 2: CẤU HÌNH NODE ===
    with gr.Tab("⚙️ Quản Lý Cấu Hình (Etcd)"):
        gr.Markdown("Thay đổi hành vi của Agent mà không cần khởi động lại.")
        
        with gr.Row():
            # Nút làm mới danh sách node
            refresh_btn_1 = gr.Button("🔄 Quét tìm Node Online")
            node_dropdown_1 = gr.Dropdown(label="Chọn Node để cấu hình", choices=[])
        
        with gr.Row():
            interval_input = gr.Number(value=5.0, label="Interval (giây)", minimum=0.1)
            metrics_input = gr.CheckboxGroup(["cpu", "memory", "disk read", "disk write", "net in", "net out"], label="Metrics thu thập", value=["cpu", "memory", "disk read", "disk write", "net in", "net out"])
            plugins_input = gr.CheckboxGroup(["deduplicate", "threshold", "average", "converter"], label="Chọn plug in", value=["deduplicate", "threshold", "average", "converter"])
        
        update_btn = gr.Button("Đẩy Cấu Hình (Push Config)", variant="primary")
        config_status = gr.Textbox(label="Trạng thái", interactive=False)

        # Sự kiện
        refresh_btn_1.click(fn=get_active_nodes, outputs=node_dropdown_1)
        update_btn.click(
            fn=push_config, 
            inputs=[node_dropdown_1, interval_input, metrics_input, plugins_input], 
            outputs=config_status
        )

    # === TAB 3: GỬI LỆNH ===
    with gr.Tab("⚡ Gửi Lệnh (Command)"):
        gr.Markdown("Gửi lệnh điều khiển tới Agent thông qua Kafka Broker.")
        
        with gr.Row():
            refresh_btn_2 = gr.Button("🔄 Quét tìm Node Online")
            node_dropdown_2 = gr.Dropdown(label="Chọn Node đích", choices=[])
        
        cmd_input = gr.Textbox(label="Nhập lệnh (Ví dụ: HELLO, STOP, RESTART)", placeholder="ALERT_HIGH_CPU")
        send_btn = gr.Button("Gửi Lệnh (Send)", variant="stop")
        cmd_status = gr.Textbox(label="Log Gửi Lệnh", interactive=False)

        # Sự kiện
        refresh_btn_2.click(fn=get_active_nodes, outputs=node_dropdown_2)
        send_btn.click(
            fn=send_command,
            inputs=[node_dropdown_2, cmd_input],
            outputs=cmd_status
        )

# Khởi chạy App
if __name__ == "__main__":
    # Cần share=True nếu muốn truy cập từ máy khác, 
    # nhưng lưu ý firewall. Chạy local thì không cần.
    demo.launch(server_name="0.0.0.0", server_port=7860, share=True)