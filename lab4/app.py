import gradio as gr
import etcd3
import json
import threading
import time
import datetime
from confluent_kafka import Consumer, Producer

ETCD_HOST = 'etcd-0'
ETCD_PORT = 2379
KAFKA_BOOTSTRAP = 'kafka-0:9092'
# Format: {'node1': {'time': '...', 'cpu': 10, 'ram': 20}}
node_data_store = {} 

try:
    etcd_client = etcd3.client(host=ETCD_HOST, port=ETCD_PORT)
    kafka_producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP})
except Exception as e:
    print(f"Lỗi kafka/ETCD: {e}")

def run_kafka_consumer():
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

            if host not in node_data_store:
                node_data_store[host] = {'Hostname': host}
            
            node_data_store[host][metric] = val
            node_data_store[host]['Last Update'] = ts
            
        except Exception as e:
            print(f"Error parsing msg: {e}")

threading.Thread(target=run_kafka_consumer, daemon=True).start()


def get_active_nodes():
    try:
        active_nodes = []
        result = etcd_client.get_prefix("/monitor/heartbeat/")
        
        for value, meta in result:
            key = meta.key.decode('utf-8')
            # Key format: /monitor/heartbeat/<hostname>
            node_name = key.split('/')[-1]
            active_nodes.append(node_name)
        
        unique_nodes = list(set(active_nodes))
        
        if not unique_nodes:
            return gr.Dropdown(choices=[], value=None, label="Không tìm thấy Node nào")
        
        return gr.Dropdown(choices=unique_nodes, value=unique_nodes[0], label="Chọn Node để cấu hình")
        
    except Exception as e:
        return gr.Dropdown(choices=[], value=None, label=f"Lỗi etcd: {str(e)}")

def update_dashboard():
    data_list = []
    for host, info in node_data_store.items():
        cpu = info.get('cpu', 'N/A')
        mem = info.get('memory', 'N/A')
        diskRead = info.get('disk read', 'N/A')
        diskWrite = info.get('disk write', 'N/A')
        netIn = info.get('net in', 'N/A')
        netOut = info.get('net out', 'N/A')
        ts = info.get('Last Update', 'N/A')
        
        if isinstance(cpu, float): cpu = round(cpu, 2)
        if isinstance(mem, float): mem = round(mem, 2)
        if isinstance(diskRead, float): diskRead = round(mem, 2)
        if isinstance(diskWrite, float): diskWrite = round(mem, 2)
        if isinstance(netIn, float): netIn = round(mem, 2)
        if isinstance(netOut, float): netOut = round(mem, 2)
        # if isinstance(ts, str) and ts != 'N/A':
        #     ts = datetime.datetime.fromtimestamp(float(ts)).strftime('%Y-%m-%d %H:%M:%S')
        
        data_list.append([host, cpu, mem, diskRead, diskWrite, netIn, netOut, ts])
    
    return data_list

def push_config(node_name, interval, metrics, plugins):
    if not node_name or node_name == "Chưa tìm thấy Node nào":
        return "Chọn Node hợp lệ!"
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
        return f"Thành công: Đã cập nhật cho {node_name}!\nConfig: {json.dumps(config_payload)}"
    except Exception as e:
        return f"Lỗi: {str(e)}"

def send_command(node_name, command_text):
    if not node_name or node_name == "Chưa tìm thấy Node nào":
        return "Chọn Node hợp lệ!"
    
    try:
        payload = json.dumps({
            "target": node_name,
            "action": command_text
        })
        kafka_producer.produce('commands', payload.encode('utf-8'))
        kafka_producer.flush()
        return f"Đã gửi lệnh '{command_text}' tới {node_name}"
    except Exception as e:
        return f"Lỗi Kafka: {str(e)}"



with gr.Blocks(title="Distributed Monitor Admin") as demo:
    gr.Markdown("# Admin Dashboard")
    
    # === TAB 1: DASHBOARD (STREAMING MODE) ===
    with gr.Tab("Live Dashboard"):
        output_table = gr.Dataframe(
            headers=["Hostname", "CPU (%)", "Memory (%)", "Disk Read (MB)", "Disk Write (MB)", "Net In (MB)", "Net Out (MB)", "Last Timestamp"],
            datatype=["str", "number", "number", "number", "number", "number", "number", "datetime"],
            interactive=False
        )

        def stream_dashboard():
            while True:
                data = update_dashboard() 
                yield data
                time.sleep(0.1)

        demo.load(stream_dashboard, outputs=output_table)

    # === TAB 2: CẤU HÌNH NODE ===
    with gr.Tab("Etcd Config"):
        with gr.Row():
            refresh_btn_1 = gr.Button("Quét")
            node_dropdown_1 = gr.Dropdown(label="Chọn Node để cấu hình", choices=[])
        
        with gr.Row():
            interval_input = gr.Number(value=5.0, label="Interval (giây)", minimum=1.0, maximum=60.0, step=1.0)
            metrics_input = gr.CheckboxGroup(["cpu", "memory", "disk read", "disk write", "net in", "net out"], label="Metrics", value=["cpu", "memory", "disk read", "disk write", "net in", "net out"])
            plugins_input = gr.CheckboxGroup(["deduplicate", "threshold", "average", "converter"], label="Plugins", value=["deduplicate", "threshold", "average", "converter"])
        
        update_btn = gr.Button("Push Config", variant="primary")
        config_status = gr.Textbox(label="Trạng thái", interactive=False)

        refresh_btn_1.click(fn=get_active_nodes, outputs=node_dropdown_1)
        update_btn.click(
            fn=push_config, 
            inputs=[node_dropdown_1, interval_input, metrics_input, plugins_input], 
            outputs=config_status
        )

    # === TAB 3: GỬI LỆNH ===
    with gr.Tab("Command"):
        with gr.Row():
            refresh_btn_2 = gr.Button("Quét")
            node_dropdown_2 = gr.Dropdown(label="Chọn Node đích", choices=[])
        
        cmd_input = gr.Textbox(label="Nhập lệnh (Ví dụ: HELLO, STOP, RESTART)", placeholder="ALERT_HIGH_CPU")
        send_btn = gr.Button("Gửi", variant="stop")
        cmd_status = gr.Textbox(label="Log", interactive=False)

        refresh_btn_2.click(fn=get_active_nodes, outputs=node_dropdown_2)
        send_btn.click(
            fn=send_command,
            inputs=[node_dropdown_2, cmd_input],
            outputs=cmd_status
        )

if __name__ == "__main__":
    demo.launch(server_name="0.0.0.0", server_port=7860, share=True)