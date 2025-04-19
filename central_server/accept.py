from datetime import datetime
import socket
import os
from encode import encode_video

HOST = '0.0.0.0'
PORT = 8080
CHUNK_SIZE = 4096
RECEIVED_DIR = "received_videos"

def receive_file():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind((HOST, PORT))
        server_socket.listen(5)
        print(f"Server listening on {HOST}:{PORT}...")

        while True:
            client_socket, addr = server_socket.accept()
            print(f"Connection from {addr}")

            # Receive the filename first (fixed length of 256 bytes)
            raw_name = client_socket.recv(256).strip()
            filename = raw_name.decode()
            safe_filename = os.path.basename(filename)
            output_path = os.path.join(RECEIVED_DIR, safe_filename)

            with open(output_path, 'wb') as file:
                while True:
                    data = client_socket.recv(CHUNK_SIZE)
                    if not data:
                        break
                    file.write(data)

            print(f"File received and saved as {output_path}")
            encode_video(output_path)

            client_socket.close()

if __name__ == "__main__":
    os.makedirs("received_videos", exist_ok=True)
    receive_file()