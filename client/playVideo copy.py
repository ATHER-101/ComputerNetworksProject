import socket
import threading
import time
import os
import cv2

# Configuration
BUFFER_FOLDER = "buffer"
BUFFER_WINDOW = 2
QUALITY = "240p"
SEGMENT_PREFIX = QUALITY + "_"
CDN_IP = '127.0.0.1'
CDN_PORT = 13752
CHUNK_SIZE = 4096

# Shared state
downloaded_segments = set()
segment_lock = threading.Lock()
next_segment_to_play = 0
stop_flag = False
MAX_SEGMENT_INDEX = -1

# === Helpers ===

def get_manifest():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(('127.0.0.1', 8080))
        s.sendall(b"GIVE MANIFEST")
        return s.recv(4096).decode()

def parse_manifest(manifest_text, quality):
    for line in manifest_text.splitlines():
        if f"_{quality}.ts" in line:
            return line.split("=>")[1].strip()
    return None

def get_segment_count(quality):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((CDN_IP, CDN_PORT))
            s.sendall(f"GET_SEGMENT_COUNT {quality}".encode())
            return int(s.recv(1024).decode())
    except Exception as e:
        print(f"[Client] Failed to get segment count: {e}")
        return -1

def download_segment(segment_name):
    temp_path = os.path.join(BUFFER_FOLDER, f"tmp_{segment_name}")
    final_path = os.path.join(BUFFER_FOLDER, segment_name)

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((CDN_IP, CDN_PORT))
            s.sendall(f"GET_SEGMENT {segment_name}".encode())
            with open(temp_path, "wb") as f:
                bytes_downloaded = 0
                while True:
                    data = s.recv(CHUNK_SIZE)
                    if not data:
                        break
                    f.write(data)
                    bytes_downloaded += len(data)

        os.rename(temp_path, final_path)
        print(f"[Downloader] Downloaded {segment_name} ({bytes_downloaded} bytes)")
    except Exception as e:
        print(f"[Downloader] Failed to download {segment_name}: {e}")
        if os.path.exists(temp_path):
            os.remove(temp_path)

# === Thread Functions ===

def downloader_thread():
    global stop_flag
    while not stop_flag:
        with segment_lock:
            segment_idx = next_segment_to_play
            needed_segments = [
                f"{SEGMENT_PREFIX}{i:03d}.ts"
                for i in range(segment_idx, min(segment_idx + BUFFER_WINDOW, MAX_SEGMENT_INDEX + 1))
            ]
            existing = set(os.listdir(BUFFER_FOLDER))
            to_download = [seg for seg in needed_segments if seg not in existing]

        for seg in to_download:
            if stop_flag:
                break
            download_segment(seg)
            with segment_lock:
                downloaded_segments.add(seg)

        time.sleep(0.5)

def player_thread():
    global next_segment_to_play, stop_flag
    while not stop_flag and next_segment_to_play <= MAX_SEGMENT_INDEX:
        segment_name = f"{SEGMENT_PREFIX}{next_segment_to_play:03d}.ts"
        segment_path = os.path.join(BUFFER_FOLDER, segment_name)

        if not os.path.exists(segment_path):
            print(f"[Player] Waiting for segment {segment_name}...")
            time.sleep(0.5)
            continue

        cap = cv2.VideoCapture(segment_path)
        retry_count = 5
        while not cap.isOpened() and retry_count > 0:
            print(f"[Player] Could not open {segment_name}, retrying...")
            time.sleep(0.5)
            cap = cv2.VideoCapture(segment_path)
            retry_count -= 1

        if not cap.isOpened():
            print(f"[Player] Failed to open {segment_name} after retries.")
            stop_flag = True
            break

        while True:
            ret, frame = cap.read()
            if not ret:
                break
            frame = cv2.resize(frame, (480, 270))
            cv2.imshow("Streaming Video", frame)
            if cv2.waitKey(25) & 0xFF == ord('q'):
                stop_flag = True
                cap.release()
                cv2.destroyAllWindows()
                return

        cap.release()
        os.remove(segment_path)
        print(f"[Player] Finished {segment_name}, deleted from buffer.")
        next_segment_to_play += 1

    print("[Player] Reached end of stream.")
    stop_flag = True
    cv2.destroyAllWindows()

# === Main Logic ===

if __name__ == "__main__":
    os.makedirs(BUFFER_FOLDER, exist_ok=True)

    print("[Client] Getting manifest...")
    manifest = get_manifest()
    addr = parse_manifest(manifest, QUALITY)
    if not addr:
        print("[Client] Could not find quality in manifest.")
        exit()

    MAX_SEGMENT_INDEX = get_segment_count(QUALITY) - 1
    if MAX_SEGMENT_INDEX < 0:
        print("[Client] Failed to determine segment count. Exiting.")
        exit()

    print(f"[Client] Streaming {QUALITY} with {MAX_SEGMENT_INDEX + 1} segments...")

    # Start downloader thread
    downloader = threading.Thread(target=downloader_thread, daemon=True)
    downloader.start()

    # Wait for initial buffer
    print(f"[Client] Buffering first {BUFFER_WINDOW} segments before playback...")
    initial_segments = [f"{SEGMENT_PREFIX}{i:03d}.ts" for i in range(BUFFER_WINDOW)]
    while True:
        ready = all(os.path.exists(os.path.join(BUFFER_FOLDER, seg)) for seg in initial_segments)
        if ready:
            print("[Client] Initial buffer filled. Starting playback...")
            break
        print("[Client] Waiting for buffer to fill...")
        time.sleep(0.5)

    # Run player in main thread (to support OpenCV GUI on macOS)
    player_thread()

    # Join downloader on exit
    downloader.join()