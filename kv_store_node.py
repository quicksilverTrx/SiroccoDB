
import socket
import threading
import pickle
1


class DistributedKeyValueStoreNode:
    def __init__(self, host, port, role = "follower", leader_host = None, leader_port = None):
        self.host = host
        self.port = port
        self.role = role
        self.leader_host = leader_host
        self.leader_port = leader_port
        self.data = {}
        self.followers = []
        self.lock = threading.Lock()

    def handle_client_request(self, client_socket, client_address):
        client_socket.setblocking(False)
        try:
            request = client_socket.recv(4096)
        except BlockingIOError:
            return None
        command,key,value = pickle.loads(request)
        if self.role == 'leader':
            if command == 'SET':
                self.set_key(key,value)
                # replicate to followers
                self.replicate_to_followers(command,key,value)
                response = "OK"
            elif command == 'GET':
                response = self.get_key(key)
            elif command == 'DELETE':
                self.delete_key(key)
                # replicate to followers
                self.replicate_to_followers(command,key)
                response = "OK"
        else:
            #follower
            if command == "GET":
                response = self.get_key(key)
            else:
                response = "followers can only handle get requests"
        client_socket.sendall(pickle.dumps(response))
        client_socket.close()
        
                

    def set_key(self, key, value):
        with self.lock:
            self.data[key] = value

    def get_key(self, key):
        with self.lock:
            return self.data.get(key, "Key not found")
    
    def delete_key(self, key):
        with self.lock:
            self.data.pop(key, None)

    def replicate_to_followers(self, command, key, value=None):
        for follower in self.followers:
            try:
                self.send_command_to_follower(follower[0],follower[1], command, key, value)
            except Exception as e:
                print(f"Error replicating to follower {follower}: {e}")

    def send_commmand_to_follower(self,host,port,command,key,value=None):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as follower_socket:
            follower_socket.connect((host,port))
            follower_socket.sendall(pickle.dumps((command,key,value)))
            response = follower_socket.recv(4096)
            return pickle.loads(response)
        
    def start_server(self):
        server = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        server.bind((self.host,self.port))
        server.listen(10)
        print (f'{self.role.capitalize()} Server Node  started on {self.host}:{self.port}')
        while True:
            client_socket, client_address = server.accept()
            client_handlder = threading.Thread(target=self.handle_client_request, args=(client_socket,))
            client_handlder.start()

    def add_follower(self, follower_host, follower_port):
        self.followers.append((follower_host, follower_port))

def start_node(host,port,role = 'follower', leader_host = None, leader_port = None):
    node = DistributedKeyValueStoreNode(host, port, role, leader_host, leader_port)
    if role == 'leader' and leader_host = None:
        node.start_server() 
    elif node == 'follower' and leader_host != None and leader_port != None:
        node.add_follower(leader_host, leader_port)
        node.start_server()

if __name__ == '__main__':
    import sys

    #example usage
    #python kv_store_node.py <host> <port> <role> <leader_host> <leader_port>
    host = sys.argv[1]
    port = int(sys.argv[2])
    role = sys.argv[3]
    leader_host = None if role == 'leader' else sys.argv[4]
    leader_port = None if role == 'leader' else int(sys.argv[5])

    start_node(host, port, role, leader_host, leader_port)


