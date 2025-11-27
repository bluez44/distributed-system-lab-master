import grpc
from concurrent import futures
import time
import monitoring_pb2
import monitoring_pb2_grpc

class MonitorService(monitoring_pb2_grpc.MonitorServiceServicer):
    
    def StreamMetrics(self, request_iterator, context):
        for metric_data in request_iterator:
            print(f"{metric_data.timestamp} - Received from {metric_data.hostname}: {metric_data.metric} = {metric_data.value}")
            
            if metric_data.metric == "cpu" and metric_data.value > 5:
                print("High CPU detected! Sending warning...")
                yield monitoring_pb2.CommandMessage(
                    command_id="CMD001", 
                    action="WARNING_HIGH_CPU"
                )
                

def serve():
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