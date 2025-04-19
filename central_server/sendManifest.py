import socket

def serve_manifest():
    HOST = '127.0.0.1'
    PORT = 8080

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, PORT))
        s.listen()
        print(f"[Central Server] Listening on {HOST}:{PORT}")

        while True:
            conn, addr = s.accept()
            with conn:
                print(f"[Central Server] Connection from {addr}")
                request = conn.recv(1024).decode().strip()
                if request == "GIVE MANIFEST":
                    try:
                        with open("manifest.txt", "r") as f:
                            manifest_data = f.read()
                            conn.sendall(manifest_data.encode())
                            print("[Central Server] Sent manifest.")
                    except FileNotFoundError:
                        conn.sendall(b"ERROR: Manifest not found.")
                        print("[Central Server] Manifest file not found.")
                    except Exception as e:
                        conn.sendall(f"ERROR: {str(e)}".encode())
                        print(f"[Central Server] Error: {e}")

if __name__ == "__main__":
    serve_manifest()