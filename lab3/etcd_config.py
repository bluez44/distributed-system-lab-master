import etcd3
import json
import sys

ETCD_HOST = 'etcd-0'
ETCD_PORT = 2379

def get_etcd_client():
    try:
        return etcd3.client(host=ETCD_HOST, port=ETCD_PORT)
    except Exception as e:
        print(f"Lỗi kết nối etcd: {e}")
        sys.exit(1)

def get_active_nodes(client):
    active_nodes = set()
    try:
        for value, meta in client.get_prefix('/monitor/heartbeat/'):
            key = meta.key.decode('utf-8')
            # Key dạng: /monitor/heartbeat/<hostname> -> Lấy hostname
            hostname = key.split('/')[-1]
            active_nodes.add(hostname)
    except Exception as e:
        pass
    
    return list(active_nodes)

def update_config(client):
    nodes = get_active_nodes(client)
    
    if not nodes:
        print("Bạn có muốn nhập tên node thủ công không?")
        choice = input("Chọn (y/n): ").lower()
        if choice != 'y': return
        target_node = input("Nhập tên node (hostname): ").strip()
    else:
        print("\n=== DANH SÁCH NODE ===")
        for idx, node in enumerate(nodes):
            print(f"{idx + 1}. {node}")
        print("======================")
        
        try:
            selection = int(input(f"Chọn node (1-{len(nodes)}): "))
            if 1 <= selection <= len(nodes):
                target_node = nodes[selection - 1]
            else:
                print("Lựa chọn không hợp lệ.")
                return
        except ValueError:
            print("Vui lòng nhập số.")
            return

    print(f"\nĐang cấu hình cho: [{target_node}]")
    
    try:
        interval = float(input("1. Nhập Interval (giây): "))
    except ValueError:
        print("Interval phải là số.")
        return

    print("2. Nhập Metrics (cách nhau bởi dấu phẩy cpu,memory,disk write, disk read, net in, net out):")
    metrics_input = input("   Metrics: ").strip()
    metrics_list = [m.strip() for m in metrics_input.split(',')] if metrics_input else ["cpu"]

    config_data = {
        "interval": interval,
        "metrics": metrics_list,
    }
    
    json_str = json.dumps(config_data)
    key = f"/monitor/config/{target_node}"

    try:
        client.put(key, json_str)
        print("Cấu hình thành công!")
    except Exception as e:
        print(f"Lỗi khi đẩy config: {e}")

if __name__ == "__main__":
    client = get_etcd_client()
    while True:
        update_config(client)
        print("\n--------------------------------")
        cont = input("Tiếp tục cấu hình node khác? (y/n): ")
        if cont.lower() != 'y':
            break