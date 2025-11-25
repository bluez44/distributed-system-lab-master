import grpc
from concurrent import futures
import time
import monitoring_pb2
import monitoring_pb2_grpc

class MonitorService(monitoring_pb2_grpc.MonitorServiceServicer):
    
    def StreamMetrics(self, request_iterator, context):
        """
        Xử lý Bidirectional Streaming.
        request_iterator: Luồng dữ liệu từ Client gửi lên.
        Hàm này cần 'yield' để gửi lệnh về Client.
        """
        for metric_data in request_iterator:
            # 1. Xử lý dữ liệu nhận được
            
            print(f"{datetime.fromtimestamp(timestamp)} - Received from {metric_data.hostname}: {metric_data.metric} = {metric_data.value}")
            
            # 2. Logic gửi lệnh về Client (Demo)
            # Ví dụ: Nếu CPU > 80%, gửi lệnh cảnh báo
            if metric_data.metric == "cpu" and metric_data.value > 80:
                print("High CPU detected! Sending warning...")
                yield monitoring_pb2.CommandMessage(
                    command_id="CMD001", 
                    action="WARNING_HIGH_CPU"
                )
                
            # Lưu ý: Trong thực tế, bạn có thể lưu request_iterator vào hàng đợi 
            # để xử lý bất đồng bộ, nhưng code này giữ đơn giản để chạy demo.

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