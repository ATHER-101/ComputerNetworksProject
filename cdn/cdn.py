# cdn.py
import socket
import os
import threading

# Local cache directory for videos.
CACHE_DIR = "cdn_cache"
os.makedirs(CACHE_DIR, exist_ok=True)

# IP and port settings.
DISTRIBUTION_IP = '127.0.0.1'
DISTRIBUTION_PORT = 6000  # Where distribution.py is listening.
CLIENT_REQUEST_PORT = 8000  # For client requests.
PUSH_LISTENER_PORT = 54321   # For pushed files from distribution.

##################################
# 1. Pulling from Distribution  #
##################################
def pull_video_from_distribution(video_name):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((DISTRIBUTION_IP, DISTRIBUTION_PORT))

        # For pull, send mode 0 then video name (length-prefixed).
        s.send((0).to_bytes(1, 'big'))
        name_bytes = video_name.encode()
        s.send(len(name_bytes).to_bytes(1, 'big'))
        s.send(name_bytes)

        # Receive file size.
        file_size = int.from_bytes(s.recv(8), 'big')
        if file_size == 0:
            print(f"[CDN] Distribution reported missing video '{video_name}'.")
            s.close()
            return None  # Explicitly return None on failure

        os.makedirs(CACHE_DIR, exist_ok=True)
        file_path = os.path.join(CACHE_DIR, video_name)

        video_data = b''
        bytes_received = 0
        while bytes_received < file_size:
            chunk = s.recv(min(4096, file_size - bytes_received))
            if not chunk:
                break
            video_data += chunk
            bytes_received += len(chunk)

        # Save to cache
        with open(file_path, 'wb') as f:
            f.write(video_data)

        s.close()
        print(f"[CDN] Pulled and cached '{video_name}' from Distribution.")
        return video_data  # Return the data to be sent to client

    except Exception as e:
        print(f"[CDN] Error pulling video '{video_name}':", e)
        return None

##################################
# 2. Client Request Server       #
##################################
def handle_client_request(client_socket):
    try:
        # Client sends video name: 1-byte length then the video name.
        name_len = int.from_bytes(client_socket.recv(1), 'big')
        video_name = client_socket.recv(name_len).decode()
        print(f"[CDN] Client requested '{video_name}'")
        file_path = os.path.join(CACHE_DIR, video_name)
        # If video not in cache, pull it from distribution.
        if not os.path.exists(file_path):
            print(f"[CDN] '{video_name}' not in cache. Pulling from Distribution.")
            pull_video_from_distribution(video_name)
        # Check again and, if available, send to client.
        if not os.path.exists(file_path):
            print(f"[CDN] '{video_name}' still unavailable after pull.")
            client_socket.send((0).to_bytes(8, 'big'))
            client_socket.close()
            return
        file_size = os.path.getsize(file_path)
        client_socket.send(file_size.to_bytes(8, 'big'))
        with open(file_path, 'rb') as f:
            while True:
                data = f.read(4096)
                if not data:
                    break
                client_socket.sendall(data)
        client_socket.close()
        print(f"[CDN] Served '{video_name}' to client.")
    except Exception as e:
        print("[CDN] Error handling client request:", e)
        client_socket.close()

def client_request_server(port=7000):
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.bind(('', port))
    server_sock.listen(5)
    print(f"[CDN] Listening for client requests on port {port}")
    while True:
        client_socket, addr = server_sock.accept()
        threading.Thread(target=handle_client_request, args=(client_socket,), daemon=True).start()

##################################
# 3. Push Listener for Distribution  #
##################################
def push_listener(port=5000):
    """
    Listens for push messages from Distribution.
    Protocol for push: first byte is mode (should be 1), then 1-byte video name length,
    the video name, 8-byte file size, then file data.
    """
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.bind(('', port))
    server_sock.listen(5)
    print(f"[CDN] Listening for pushed videos on port {port}")
    while True:
        client_socket, addr = server_sock.accept()
        try:
            cmd = int.from_bytes(client_socket.recv(1), 'big')
            if cmd != 1:
                print("[CDN] Unknown push command received.")
                client_socket.close()
                continue
            name_len = int.from_bytes(client_socket.recv(1), 'big')
            video_name = client_socket.recv(name_len).decode()
            file_size = int.from_bytes(client_socket.recv(8), 'big')
            file_path = os.path.join(CACHE_DIR, video_name)
            with open(file_path, 'wb') as f:
                bytes_received = 0
                while bytes_received < file_size:
                    chunk = client_socket.recv(min(4096, file_size - bytes_received))
                    if not chunk:
                        break
                    f.write(chunk)
                    bytes_received += len(chunk)
            client_socket.close()
            print(f"[CDN] Received pushed video '{video_name}' and saved to cache.")
        except Exception as e:
            print("[CDN] Error in push listener:", e)
            client_socket.close()

##################################
# Main: Run both servers         #
##################################
if __name__ == "__main__":
    # Start the push listener in a separate thread.
    threading.Thread(target=push_listener, args=(PUSH_LISTENER_PORT,), daemon=True).start()
    # Start the client request server (main thread or its own thread).
    client_request_server(CLIENT_REQUEST_PORT)
