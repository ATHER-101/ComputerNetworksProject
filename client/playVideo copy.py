import sys
import os
import socket
import threading
import time
import cv2
import numpy as np

from PyQt5.QtWidgets import QSizePolicy, QApplication, QWidget, QVBoxLayout, QLabel, QPushButton, QSlider, QHBoxLayout, QMessageBox
from PyQt5.QtGui import QImage, QPixmap
from PyQt5.QtCore import Qt, QTimer

# Configuration
BUFFER_FOLDER = "buffer"
BUFFER_WINDOW = 2
CHUNK_SIZE = 4096
WINDOW_WIDTH = 700
WINDOW_HEIGHT = 550
VIDEO_LABEL_WIDTH = 700
VIDEO_LABEL_HEIGHT = 400
SEGMENT_DURATION = 4
SEEK_DEBOUNCE_TIME = 0.1
DOWNLOAD_TIMEOUT_1 = 1
DOWNLOAD_TIMEOUT_2 = 3
ABR_RESET_DELAY = 30
QUALITY_LEVELS = ["720p", "360p", "240p"]
CDN_LIST = [
    ('127.0.0.1', 13751),
    ('127.0.0.1', 13752),
]

# Global State
downloaded_segments = set()
segment_lock = threading.Lock()
next_segment_to_play = 0
stop_flag = False
playback_state = {
    "playing": True,
    "seek_to": None,
    "exit": False,
    "abr_enabled": True,
    "last_explicit_quality_change": 0
}
manifest_dict = None
current_video_name = None
current_quality = "720p"
pending_quality = None
CDN_IP = None
CDN_PORT = None
MAX_SEGMENT_INDEX = -1
TOTAL_DURATION = 0
last_seek_time = 0
last_download_time = 0
last_playback_time = 0
last_not_found_log = 0

# Helper Functions
def get_manifest():
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(('127.0.0.1', 8090))
            s.sendall(b"GIVE MANIFEST")
            manifest_text = s.recv(4096).decode()
            print(f"[Client] Raw manifest: {manifest_text}")
            return manifest_text
    except Exception as e:
        print(f"[Client] Failed to get manifest: {e}")
        return None

def parse_manifest(manifest_text):
    manifest_dict = {}
    if not manifest_text or manifest_text.strip() == "":
        print("[Client] Manifest is empty.")
        return manifest_dict
    for line in manifest_text.splitlines():
        if "=>" in line:
            key, value = line.split("=>")
            video_quality = key.strip()
            video_name, quality_with_ext = video_quality.split("_")
            quality = quality_with_ext.replace(".ts", "")
            ip, port = value.strip().split(":")
            if video_name not in manifest_dict:
                manifest_dict[video_name] = {}
            if quality not in manifest_dict[video_name]:
                manifest_dict[video_name][quality] = []
            manifest_dict[video_name][quality].append((ip, int(port)))
    return manifest_dict

def measure_rtt(ip, port):
    start_time = time.time()
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(1)
            s.connect((ip, port))
        end_time = time.time()
        return end_time - start_time
    except Exception:
        return float('inf')

def find_fastest_cdn():
    rtt_list = [(cdn, measure_rtt(cdn[0], cdn[1])) for cdn in CDN_LIST]
    valid_cdns = [(cdn, rtt) for cdn, rtt in rtt_list if rtt != float('inf')]
    if not valid_cdns:
        print("[Client] No CDNs are reachable.")
        return None
    fastest_cdn = min(valid_cdns, key=lambda x: x[1])[0]
    print(f"[Client] Selected fastest CDN {fastest_cdn[0]}:{fastest_cdn[1]} with RTT {min(valid_cdns, key=lambda x: x[1])[1]:.4f}s")
    return fastest_cdn

def get_segment_count(ip, port):
    for cdn_ip, cdn_port in manifest_dict[current_video_name][current_quality]:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((cdn_ip, cdn_port))
                s.sendall(f"GET_SEGMENT_COUNT {current_video_name} {current_quality}".encode())
                count = int(s.recv(1024).decode()) - 1
                print(f"[Client] Received segment count {count + 1} for {current_video_name} ({current_quality}) from {cdn_ip}:{cdn_port}")
                return count
        except Exception as e:
            print(f"[Client] Failed to get segment count from {cdn_ip}:{cdn_port}: {e}")
    print(f"[Client] All CDNs failed for {current_video_name} ({current_quality}).")
    return -1

def download_segment(segment_idx, quality):
    global last_download_time
    segment_name = f"{segment_idx:03d}.ts"
    save_name = f"{quality}_{segment_name}"
    temp_path = os.path.join(BUFFER_FOLDER, f"tmp_{save_name}")
    final_path = os.path.join(BUFFER_FOLDER, save_name)
    ip, port = CDN_IP, CDN_PORT

    start_time = time.time()
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((ip, port))
            s.sendall(f"GET_SEGMENT {current_video_name} {quality} {segment_name}".encode())
            with open(temp_path, "wb") as f:
                while True:
                    data = s.recv(CHUNK_SIZE)
                    if not data:
                        break
                    f.write(data)

        if os.path.exists(temp_path):
            os.rename(temp_path, final_path)
            last_download_time = time.time()
            print(f"[Downloader] Downloaded {save_name} in {last_download_time - start_time:.2f}s")
        else:
            print(f"[Downloader] Temp file {temp_path} missing after download.")
            return
    except Exception as e:
        print(f"[Downloader] Failed to download {save_name}: {e}")
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except Exception as rm_e:
                print(f"[Downloader] Failed to remove temp file {temp_path}: {rm_e}")

# Downloader Thread
def downloader_thread():
    global stop_flag, next_segment_to_play, pending_quality, current_quality
    while not stop_flag and not playback_state["exit"]:
        with segment_lock:
            quality_to_download = pending_quality if pending_quality else current_quality
            if playback_state["seek_to"] is not None:
                seek_timestamp = playback_state["seek_to"]
                seek_segment = int(seek_timestamp // SEGMENT_DURATION)
                existing = set(os.listdir(BUFFER_FOLDER))
                if f"{quality_to_download}_{seek_segment:03d}.ts" not in existing:
                    download_segment(seek_segment, quality_to_download)
                    downloaded_segments.add(f"{quality_to_download}_{seek_segment:03d}.ts")
                next_segment_to_play = seek_segment
                playback_state["seek_to"] = None

            segment_idx = next_segment_to_play
            needed_segments = list(range(segment_idx, min(segment_idx + BUFFER_WINDOW, MAX_SEGMENT_INDEX + 1)))
            existing = set(os.listdir(BUFFER_FOLDER))
            to_download = [i for i in needed_segments if f"{quality_to_download}_{i:03d}.ts" not in existing]

        for i in to_download:
            if stop_flag or playback_state["exit"]:
                break
            download_segment(i, quality_to_download)
            with segment_lock:
                downloaded_segments.add(f"{quality_to_download}_{i:03d}.ts")

        time.sleep(0.5)

# GUI Class
class VideoPlayer(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("PyQt5 CDN Video Player")
        self.setFixedSize(WINDOW_WIDTH, WINDOW_HEIGHT)

        self.video_label = QLabel()
        self.video_label.setAlignment(Qt.AlignCenter)
        self.video_label.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.video_label.setStyleSheet("background-color: black;")

        self.play_button = QPushButton("Pause")
        self.play_button.setFixedSize(100, 35)
        self.play_button.setStyleSheet("""
            QPushButton {
                background-color: #2196F3;
                color: white;
                border-radius: 5px;
                padding: 5px;
            }
            QPushButton:hover {
                background-color: #1976D2;
            }
        """)

        self.slider = QSlider(Qt.Horizontal)
        self.slider.setTracking(True)
        self.slider.setStyleSheet("""
            QSlider::groove:horizontal {
                height: 8px;
                background: #B0BEC5;
                border-radius: 4px;
            }
            QSlider::handle:horizontal {
                background: #2196F3;
                width: 16px;
                height: 16px;
                margin: -4px 0;
                border-radius: 8px;
            }
            QSlider::sub-page:horizontal {
                background: #4CAF50;
                border-radius: 4px;
            }
        """)

        self.play_button.clicked.connect(self.toggle_play)
        self.slider.valueChanged.connect(self.slider_moved)

        quality_layout = QHBoxLayout()
        quality_layout.setSpacing(10)
        self.button_240p = QPushButton("240p")
        self.button_240p.setFixedSize(80, 30)
        self.button_240p.setStyleSheet("""
            QPushButton {
                background-color: #4CAF50;
                color: white;
                border-radius: 5px;
            }
            QPushButton:hover {
                background-color: #388E3C;
            }
        """)
        self.button_240p.clicked.connect(lambda: self.set_quality("240p", user_initiated=True))
        quality_layout.addWidget(self.button_240p)

        self.button_360p = QPushButton("360p")
        self.button_360p.setFixedSize(80, 30)
        self.button_360p.setStyleSheet("""
            QPushButton {
                background-color: #4CAF50;
                color: white;
                border-radius: 5px;
            }
            QPushButton:hover {
                background-color: #388E3C;
            }
        """)
        self.button_360p.clicked.connect(lambda: self.set_quality("360p", user_initiated=True))
        quality_layout.addWidget(self.button_360p)

        self.button_720p = QPushButton("720p")
        self.button_720p.setFixedSize(80, 30)
        self.button_720p.setStyleSheet("""
            QPushButton {
                background-color: #4CAF50;
                color: white;
                border-radius: 5px;
            }
            QPushButton:hover {
                background-color: #388E3C;
            }
        """)
        self.button_720p.clicked.connect(lambda: self.set_quality("720p", user_initiated=True))
        quality_layout.addWidget(self.button_720p)

        control_bar = QHBoxLayout()
        control_bar.setSpacing(20)
        control_bar.addWidget(self.play_button)
        control_bar.addStretch()
        control_bar.addLayout(quality_layout)
        control_bar.addStretch()

        layout = QVBoxLayout()
        layout.setSpacing(20)
        layout.setContentsMargins(20, 20, 20, 20)
        layout.addWidget(self.video_label)
        layout.addLayout(control_bar)
        layout.addWidget(self.slider)
        layout.setAlignment(Qt.AlignCenter)
        self.setLayout(layout)

        self.timer = QTimer()
        self.timer.timeout.connect(self.play_next_frame)
        self.timer.start(30)

        self.current_cap = None
        self.current_segment = -1
        self.update_quality_styles()
        self.set_quality(current_quality)

    def set_quality(self, quality, user_initiated=False):
        global pending_quality, CDN_IP, CDN_PORT, MAX_SEGMENT_INDEX, TOTAL_DURATION, current_quality

        if current_video_name not in manifest_dict or quality not in manifest_dict[current_video_name]:
            if user_initiated:
                QMessageBox.warning(self, "Error", f"Quality {quality} not available for {current_video_name}")
            print(f"[Client] Quality {quality} not in manifest for {current_video_name}. Attempting to find a CDN.")
            fastest_cdn = find_fastest_cdn()
            if not fastest_cdn:
                if user_initiated:
                    QMessageBox.warning(self, "Error", f"No CDNs available for {quality}")
                return
            if current_video_name not in manifest_dict:
                manifest_dict[current_video_name] = {}
            if quality not in manifest_dict[current_video_name]:
                manifest_dict[current_video_name][quality] = []
            manifest_dict[current_video_name][quality].append(fastest_cdn)
            print(f"[Client] Added {fastest_cdn[0]}:{fastest_cdn[1]} to manifest for {current_video_name} ({quality})")

        cdn_list = manifest_dict[current_video_name][quality]
        if not cdn_list:
            if user_initiated:
                QMessageBox.warning(self, "Error", f"No CDNs available for {quality}")
            return

        rtt_list = [(cdn, measure_rtt(cdn[0], cdn[1])) for cdn in cdn_list]
        closest_cdn = min(rtt_list, key=lambda x: x[1])[0]

        ip, port = closest_cdn
        max_segments = get_segment_count(ip, port)
        if max_segments < 0:
            if user_initiated:
                QMessageBox.warning(self, "Error", f"CDN for {quality} at {ip}:{port} is not available")
            return

        pending_quality = quality
        CDN_IP, CDN_PORT = ip, port
        MAX_SEGMENT_INDEX = max_segments
        TOTAL_DURATION = (MAX_SEGMENT_INDEX + 1) * SEGMENT_DURATION
        self.slider.setRange(0, TOTAL_DURATION)

        if user_initiated:
            playback_state["abr_enabled"] = False
            playback_state["last_explicit_quality_change"] = time.time()

        current_quality = quality
        self.update_quality_styles()
        print(f"[Client] Queued quality change to {quality}, total duration: {TOTAL_DURATION}s")

    def update_quality_styles(self):
        default_style = """
            QPushButton {
                background-color: #4CAF50;
                color: white;
                border-radius: 5px;
            }
            QPushButton:hover {
                background-color: #388E3C;
            }
        """
        selected_style = """
            QPushButton {
                background-color: #2E7D32;
                color: white;
                border-radius: 5px;
            }
            QPushButton:hover {
                background-color: #1B5E20;
            }
        """
        self.button_240p.setStyleSheet(default_style if current_quality != "240p" else selected_style)
        self.button_360p.setStyleSheet(default_style if current_quality != "360p" else selected_style)
        self.button_720p.setStyleSheet(default_style if current_quality != "720p" else selected_style)

    def toggle_play(self):
        playback_state["playing"] = not playback_state["playing"]
        self.play_button.setText("Pause" if playback_state["playing"] else "Play")

    def slider_moved(self):
        global last_seek_time
        current_time = time.time()
        if current_time - last_seek_time < SEEK_DEBOUNCE_TIME:
            return
        value = self.slider.value()
        playback_state["seek_to"] = value
        last_seek_time = current_time
        print(f"[Client] Seeking to timestamp {value}s")

    def play_next_frame(self):
        global next_segment_to_play, current_quality, pending_quality, CDN_IP, CDN_PORT, MAX_SEGMENT_INDEX, last_playback_time, last_not_found_log

        if playback_state["exit"] or not playback_state["playing"]:
            return

        current_time = time.time()
        if not playback_state["abr_enabled"] and current_time - playback_state["last_explicit_quality_change"] > ABR_RESET_DELAY:
            playback_state["abr_enabled"] = True
            print(f"[ABR] Re-enabled ABR after {ABR_RESET_DELAY}s")

        if playback_state["seek_to"] is not None:
            seek_timestamp = playback_state["seek_to"]
            next_segment_to_play = int(seek_timestamp // SEGMENT_DURATION)
            if self.current_cap:
                self.current_cap.release()
                self.current_cap = None
            self.current_segment = -1
            pending_quality = None
            print(f"[Client] Seek to segment {next_segment_to_play} from timestamp {seek_timestamp}s")
            playback_state["seek_to"] = None

        if next_segment_to_play > MAX_SEGMENT_INDEX:
            playback_state["exit"] = True
            self.close()
            return

        segment_name = f"{current_quality}_{next_segment_to_play:03d}.ts"
        segment_path = os.path.join(BUFFER_FOLDER, segment_name)

        if not os.path.exists(segment_path):
            current_time = time.time()
            if current_time - last_not_found_log >= 1:
                print(f"[Client] Segment {segment_path} not found, waiting for download")
                last_not_found_log = current_time
            if playback_state["abr_enabled"]:
                waiting_time = current_time - last_playback_time
                current_idx = QUALITY_LEVELS.index(current_quality)
                if waiting_time > DOWNLOAD_TIMEOUT_2 and current_quality != "240p":
                    pending_quality = "240p"
                    print(f"[ABR] Segment {segment_path} unavailable for {waiting_time:.2f}s (> {DOWNLOAD_TIMEOUT_2}s), downgrading directly to 240p")
                elif waiting_time > DOWNLOAD_TIMEOUT_1 and current_idx < len(QUALITY_LEVELS) - 1:
                    new_quality = QUALITY_LEVELS[current_idx + 1]
                    pending_quality = new_quality
                    print(f"[ABR] Segment {segment_path} unavailable for {waiting_time:.2f}s (> {DOWNLOAD_TIMEOUT_1}s), downgrading to {new_quality}")
            return

        if self.current_segment != next_segment_to_play:
            if self.current_cap:
                self.current_cap.release()
            self.current_cap = cv2.VideoCapture(segment_path)
            self.current_segment = next_segment_to_play
            if not self.current_cap.isOpened():
                print(f"[Client] Failed to open segment {segment_path}")
                next_segment_to_play += 1
                last_playback_time = time.time()
                return

        if not self.current_cap or not self.current_cap.isOpened():
            print(f"[Client] Invalid video capture for segment {segment_path}")
            next_segment_to_play += 1
            last_playback_time = time.time()
            return

        ret, frame = self.current_cap.read()
        if not ret or frame is None:
            self.current_cap.release()
            self.current_cap = None
            next_segment_to_play += 1
            self.slider.setValue(next_segment_to_play * SEGMENT_DURATION)
            last_playback_time = time.time()
            if pending_quality:
                current_quality = pending_quality
                pending_quality = None
                cdn_list = manifest_dict[current_video_name][current_quality]
                rtt_list = [(cdn, measure_rtt(cdn[0], cdn[1])) for cdn in cdn_list]
                closest_cdn = min(rtt_list, key=lambda x: x[1])[0]
                CDN_IP, CDN_PORT = closest_cdn
                MAX_SEGMENT_INDEX = get_segment_count(CDN_IP, CDN_PORT)
                if MAX_SEGMENT_INDEX < 0:
                    QMessageBox.warning(self, "Error", f"CDN for {current_quality} is not available")
                    playback_state["exit"] = True
                    return
                TOTAL_DURATION = (MAX_SEGMENT_INDEX + 1) * SEGMENT_DURATION
                self.slider.setRange(0, TOTAL_DURATION)
                self.update_quality_styles()
                print(f"[Client] Switched to quality {current_quality}, total duration: {TOTAL_DURATION}s")
            return

        last_playback_time = time.time()
        self.slider.setValue(next_segment_to_play * SEGMENT_DURATION)

        rgb_image = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        h, w, ch = rgb_image.shape
        bytes_per_line = ch * w
        qt_image = QImage(rgb_image.data, w, h, bytes_per_line, QImage.Format_RGB888)
        pixmap = QPixmap.fromImage(qt_image).scaled(
            VIDEO_LABEL_WIDTH, VIDEO_LABEL_HEIGHT, Qt.KeepAspectRatio, Qt.SmoothTransformation
        )
        self.video_label.setPixmap(pixmap)

# Main App Launcher
def main():
    global manifest_dict, current_video_name, CDN_IP, CDN_PORT, MAX_SEGMENT_INDEX, TOTAL_DURATION

    os.makedirs(BUFFER_FOLDER, exist_ok=True)

    print("[Client] Getting manifest...")
    manifest_text = get_manifest()
    if not manifest_text:
        print("[Client] Could not retrieve manifest. Attempting to find a CDN.")
        manifest_dict = {}
    else:
        manifest_dict = parse_manifest(manifest_text)
        print(f"[Client] Parsed manifest: {manifest_dict}")

    current_video_name = input("Enter video name (e.g., 'new', 'video'): ").strip()

    if current_video_name not in manifest_dict or "720p" not in manifest_dict[current_video_name]:
        print(f"[Client] Video '{current_video_name}' or quality '720p' not found in manifest.")
        fastest_cdn = find_fastest_cdn()
        if not fastest_cdn:
            print("[Client] No CDNs available. Exiting.")
            return
        if current_video_name not in manifest_dict:
            manifest_dict[current_video_name] = {}
        for quality in QUALITY_LEVELS:
            manifest_dict[current_video_name][quality] = [fastest_cdn]
        print(f"[Client] Added {fastest_cdn[0]}:{fastest_cdn[1]} to manifest for {current_video_name} (all qualities)")

    # Initialize with default quality (720p)
    cdn_list = manifest_dict[current_video_name]["720p"]
    if not cdn_list:
        print("[Client] No CDNs available for default quality (720p). Exiting.")
        return

    rtt_list = [(cdn, measure_rtt(cdn[0], cdn[1])) for cdn in cdn_list]
    closest_cdn = min(rtt_list, key=lambda x: x[1])[0]
    CDN_IP, CDN_PORT = closest_cdn
    MAX_SEGMENT_INDEX = get_segment_count(CDN_IP, CDN_PORT)
    if MAX_SEGMENT_INDEX < 0:
        print(f"[Client] CDN for default quality (720p) at {CDN_IP}:{CDN_PORT} not available. Exiting.")
        return
    TOTAL_DURATION = (MAX_SEGMENT_INDEX + 1) * SEGMENT_DURATION

    dl_thread = threading.Thread(target=downloader_thread, daemon=True)
    dl_thread.start()

    app = QApplication(sys.argv)
    player = VideoPlayer()
    player.show()
    app.exec_()

    playback_state["exit"] = True
    dl_thread.join()

if __name__ == "__main__":
    main()