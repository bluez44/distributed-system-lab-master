import etcd3 #version0.12.0

etcd=etcd3.client(host='etcd-0',port=2379)
KEY_PREFIX="/monitor/heartbeat/"

def on_heartbeat_event(watch_response):
    for event in watch_response.events:
        key=event.key.decode('utf-8')
        node_id=event.key.decode('utf-8').split('/')[-1]
        if isinstance(event, etcd3.events.PutEvent):
            value = event.value.decode('utf-8')
            print(f"[+] Node {node_id} alive-> {value}")
        elif isinstance(event, etcd3.events.DeleteEvent):
            print(f"[-] Node {node_id} dead (key removed)")


def monitor_heartbeats():
    watch_id=etcd.add_watch_prefix_callback(KEY_PREFIX,on_heartbeat_event)
    
    try:
        while True:
            pass #doing mainfunction
    except KeyboardInterrupt:
        print("Stopping watcher...")
        etcd.cancel_watch(watch_id)

if __name__== "__main__":
    monitor_heartbeats()