import socket
import os
import time
import random  # For jitter simulation

HOST = '127.0.0.1'
PORT = 13751
CHUNK_SIZE = 4096
VIDEO_DIR = "cdn_videos/new"
SEND_DURATION = 10  # Total delay to send each segment (in seconds)

def count_segments(quality_prefix):
    return len([f for f in os.listdir(VIDEO_DIR) if f.startswith(f"{quality_prefix}_") and f.endswith(".ts")])

def start_cdn_server():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, PORT))
        s.listen()
        print(f"[CDN] Serving segments from {VIDEO_DIR} with {SEND_DURATION}s total send time per segment")

        while True:
            conn, addr = s.accept()
            with conn:
                print(f"[CDN] Connected by {addr}")
                request = conn.recv(1024).decode().strip()

                if request.startswith("GET_SEGMENT_COUNT"):
                    _, quality = request.split()
                    count = count_segments(quality)
                    conn.sendall(str(count).encode())
                    print(f"[CDN] Sent segment count for {quality}: {count}")

                elif request.startswith("GET_SEGMENT"):
                    _, segment_name = request.split()
                    segment_path = os.path.join(VIDEO_DIR, segment_name)
                    if os.path.exists(segment_path):
                        print(f"[CDN] Sending segment: {segment_name}")
                        try:
                            with open(segment_path, "rb") as f:
                                data = f.read()
                                total_size = len(data)
                                chunks = [data[i:i+CHUNK_SIZE] for i in range(0, total_size, CHUNK_SIZE)]
                                delay_per_chunk = SEND_DURATION / len(chunks)

                                start_time = time.time()
                                for i, chunk in enumerate(chunks):
                                    try:
                                        conn.sendall(chunk)
                                        time.sleep(delay_per_chunk)

                                        elapsed = time.time() - start_time
                                        remaining = max(0, SEND_DURATION - elapsed)

                                    except BrokenPipeError:
                                        print(f"[CDN] Client disconnected during {segment_name}")
                                        break
                            print(f"[CDN] Done sending {segment_name}")
                        except Exception as e:
                            print(f"[CDN] Error sending segment {segment_name}: {e}")
                    else:
                        conn.sendall(b"ERROR: Segment not found.")
                        print(f"[CDN] Segment {segment_name} not found.")

if __name__ == "__main__":
    start_cdn_server()