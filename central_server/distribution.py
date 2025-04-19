# distribution.py
import socket
import os
import threading

# List your known CDN nodes here as (ip, port) tuples.
CDN_LIST = [
    ('127.0.0.1', 54321),
    # Add more CDN addresses as needed.
]

# Global dictionary to track the number of pull requests per video.
video_request_counts = {}
lock = threading.Lock()
MANIFEST_FILE = "manifest.txt"

def update_manifest(video_name, cdn_ip, cdn_port, manifest_path=MANIFEST_FILE):
    entry = f"{video_name} => {cdn_ip}:{cdn_port}\n"

    # Create manifest file if it doesn't exist
    if not os.path.exists(manifest_path):
        with open(manifest_path, "w") as f:
            f.write(entry)
        return

    # Avoid duplicates
    with open(manifest_path, "r") as f:
        entries = f.readlines()

    if entry not in entries:
        with open(manifest_path, "a") as f:
            f.write(entry)

def handle_connection(client_socket, client_address):
    try:
        # Expect first byte as mode. For pull requests, mode==0.
        mode_byte = client_socket.recv(1)
        if not mode_byte:
            client_socket.close()
            return
        mode = int.from_bytes(mode_byte, 'big')
        if mode != 0:
            print("[Distribution] Unknown mode received.")
            client_socket.close()
            return

        # Read video name: first the length (1 byte) then the name.
        name_len = int.from_bytes(client_socket.recv(1), 'big')
        video_name = client_socket.recv(name_len).decode()
        print(f"[Distribution] Received pull request for '{video_name}'")

        # Increment the request count (thread-safe).
        with lock:
            count = video_request_counts.get(video_name, 0) + 1
            video_request_counts[video_name] = count

        # Locate the encoded video in "encoded_videos" directory.
        file_path = os.path.join("encoded_videos", video_name)
        if not os.path.exists(file_path):
            print(f"[Distribution] Video '{video_name}' not found.")
            client_socket.send((0).to_bytes(8, 'big'))
            client_socket.close()
            return

        file_size = os.path.getsize(file_path)
        # Send file size (8 bytes)
        client_socket.send(file_size.to_bytes(8, 'big'))

        # Send file content in chunks.
        with open(file_path, 'rb') as f:
            while True:
                data = f.read(4096)
                if not data:
                    break
                client_socket.sendall(data)
        client_socket.close()
        print(f"[Distribution] Sent '{video_name}' to CDN; count = {count}")

        # Update manifest
        cdn_ip, cdn_port = client_address
        update_manifest(video_name, cdn_ip, cdn_port)

        # If the video has been requested three times, push it to all CDNs.
        if count == 3:
            print(f"[Distribution] '{video_name}' reached popularity threshold. Pushing to all CDNs.")
            push_to_all(video_name, file_path)

    except Exception as e:
        print("[Distribution] Error:", e)
        client_socket.close()

def push_to_all(video_name, file_path):
    file_size = os.path.getsize(file_path)
    for cdn_ip, cdn_port in CDN_LIST:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((cdn_ip, cdn_port))
            # Use push protocol: send mode = 1.
            s.send((1).to_bytes(1, 'big'))
            # Then send video name (length-prefixed).
            name_bytes = video_name.encode()
            s.send(len(name_bytes).to_bytes(1, 'big'))
            s.send(name_bytes)
            # Send file size (8 bytes) and file content.
            s.send(file_size.to_bytes(8, 'big'))
            with open(file_path, 'rb') as f:
                while True:
                    data = f.read(4096)
                    if not data:
                        break
                    s.sendall(data)
            s.close()
            print(f"[Distribution] Pushed '{video_name}' to CDN at {(cdn_ip, cdn_port)}")

            # Update manifest for pushed CDN
            update_manifest(video_name, cdn_ip, cdn_port)

        except Exception as e:
            print(f"[Distribution] Error pushing to {(cdn_ip, cdn_port)}: {e}")

def distribution_server(port=6000):
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.bind(('', port))
    server_sock.listen(5)
    print(f"[Distribution] Server listening on port {port}")
    while True:
        client_socket, addr = server_sock.accept()
        threading.Thread(target=handle_connection, args=(client_socket, addr), daemon=True).start()

if __name__ == "__main__":
    # Make sure the folder with encoded videos exists.
    os.makedirs("encoded_videos", exist_ok=True)
    distribution_server(6000)
