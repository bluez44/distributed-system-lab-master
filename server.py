# in server.py file
import time
import grpc
import hello_pb2
import hello_pb2_grpc
from concurrent import futures
class GreeterServicer(hello_pb2_grpc.GreeterServicer):
    def SayHello(self, request, context):
        #Unary:return oneresponse
        msg=f"Hello, {request.name}!"
        return hello_pb2.HelloReply(message=msg)

    def LotsOfReplies(self,request,context):
        #Serverstreaming: yieldmultiplemessages
        name=request.name or"friend"
        for i in range(5):
            yield hello_pb2.HelloReply(message=f"[{i}] Hi {name} from stream")
            time.sleep(0.3) # fordemopacing
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10)) #setthenumberofthreads serverareusing
    hello_pb2_grpc.add_GreeterServicer_to_server(GreeterServicer(),server)
    server.add_insecure_port("[::]:50051") #listenon allinterfacesinport50051
    server.start()
    print("gRPC server running on: 50051")
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("Shutting down...")

if __name__== "__main__":
    serve()