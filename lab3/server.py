import time
import grpc
import etcd3
import threading
from concurrent import futures
import monitoring_pb2
import monitoring_pb2_grpc

GRPC_PORT = '[::]:50051'
ETCD_HOST = 'etcd-0'

etcd = etcd3.client(host=ETCD_HOST, port=2379)

def monitor_heartbeats():
    def on_event(watch_response):
        
        for event in watch_response.events:
            key = event.key.decode()
            node = key.split("/")[-1]
            if isinstance(event, etcd3.events.PutEvent):
                print(f"[HEARTBEAT] Node {node} {event.value.decode()}")
            elif isinstance(event, etcd3.events.DeleteEvent):
                print(f"[HEARTBEAT] Node {node} dead")

    etcd.add_watch_prefix_callback("/monitor/heartbeat/", on_event)

class MonitorService(monitoring_pb2_grpc.MonitorServiceServicer):
    
    def StreamMetrics(self, request_iterator, context):
        for metric_data in request_iterator:
            print(f"[gRPC] {metric_data.timestamp} - Received from {metric_data.hostname}: {metric_data.metric} = {metric_data.value}")
            
            if metric_data.metric == "cpu" and metric_data.value > 5:
                print("High CPU detected! Sending warning...")
                yield monitoring_pb2.CommandMessage(
                    command_id="CMD001", 
                    action="WARNING_HIGH_CPU"
                )
                

def serve():
    # Chạy luồng giám sát etcd
    threading.Thread(target=monitor_heartbeats, daemon=True).start()

    # Khởi tạo gRPC Server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    monitoring_pb2_grpc.add_MonitorServiceServicer_to_server(MonitorService(), server)
    server.add_insecure_port('[::]:50051')
    print("Server started on port 50051...")
    server.start()
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()