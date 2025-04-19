import socket
import os
import time
import sys

if len(sys.argv) != 2:
    print("Usage: python cdnServer.py <port>")
    sys.exit(1)

HOST = '127.0.0.1'
PORT = int(sys.argv[1])
CHUNK_SIZE = 4096
QUALITY_DELAYS = {
    "720p": 10,  # 10 seconds delay for 720p
    "360p": 6,   # 5 seconds delay for 360p
    "240p": 3    # 1 second delay for 240p
}

def count_segments(video_name, quality):
    video_dir = f"cdn_videos/{video_name}"
    if not os.path.exists(video_dir):
        return 0
    return len([f for f in os.listdir(video_dir) if f.startswith(f"{quality}_") and f.endswith(".ts")])

def start_cdn_server():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, PORT))
        s.listen()
        print(f"[CDN] Serving segments from cdn_videos on {HOST}:{PORT}")

        while True:
            conn, addr = s.accept()
            with conn:
                print(f"[CDN] Connected by {addr}")
                request = conn.recv(1024).decode().strip()

                if request.startswith("GET_SEGMENT_COUNT"):
                    parts = request.split(maxsplit=2)
                    if len(parts) != 3:
                        conn.sendall(b"ERROR: Invalid request format.")
                        continue
                    _, video_name, quality = parts
                    count = count_segments(video_name, quality)
                    conn.sendall(str(count).encode())
                    print(f"[CDN] Sent segment count for {video_name} ({quality}): {count}")

                elif request.startswith("GET_SEGMENT"):
                    parts = request.split(maxsplit=3)
                    if len(parts) != 4:
                        conn.sendall(b"ERROR: Invalid request format.")
                        continue
                    _, video_name, quality, segment_name = parts
                    segment_path = os.path.join("cdn_videos", video_name, f"{quality}_{segment_name}")
                    if os.path.exists(segment_path):
                        # Set delay based on quality
                        send_duration = QUALITY_DELAYS.get(quality, 1)  # Default to 1s if quality not found
                        print(f"[CDN] Sending segment: {quality}_{segment_name} for {video_name} with {send_duration}s delay")
                        try:
                            with open(segment_path, "rb") as f:
                                data = f.read()
                                total_size = len(data)
                                chunks = [data[i:i+CHUNK_SIZE] for i in range(0, total_size, CHUNK_SIZE)]
                                delay_per_chunk = send_duration / len(chunks) if chunks else 0

                                for chunk in chunks:
                                    try:
                                        conn.sendall(chunk)
                                        time.sleep(delay_per_chunk)
                                    except BrokenPipeError:
                                        print(f"[CDN] Client disconnected while sending {quality}_{segment_name}")
                                        return
                            print(f"[CDN] Done sending {quality}_{segment_name} for {video_name}")
                        except Exception as e:
                            print(f"[CDN] Error sending segment {quality}_{segment_name}: {e}")
                    else:
                        conn.sendall(b"ERROR: Segment not found.")
                        print(f"[CDN] Segment {quality}_{segment_name} for {video_name} not found.")

if __name__ == "__main__":
    start_cdn_server()