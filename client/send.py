import socket
import os

SERVER_IP = "127.0.0.1"  # Change to your server's IP
PORT = 8080
CHUNK_SIZE = 4096  # 4 KB chunks

def send_file(filename):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
        try:
            client_socket.connect((SERVER_IP, PORT))
            print(f"Connected to server at {SERVER_IP}:{PORT}")

            # Send filename first (fixed-length header, 256 bytes)
            base_filename = os.path.basename(filename)
            client_socket.send(base_filename.encode().ljust(256))

            with open(filename, 'rb') as file:
                while chunk := file.read(CHUNK_SIZE):
                    client_socket.sendall(chunk)

            print("File uploaded successfully!")

        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    filename = input("Enter path to video file: ").strip()
    if not os.path.exists(filename):
        print("File not found!")
        exit(1)
    send_file(filename)