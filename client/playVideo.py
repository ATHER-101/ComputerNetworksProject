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
WINDOW_HEIGHT = 550  # Increased to 550 for more vertical space
VIDEO_LABEL_WIDTH = 700
VIDEO_LABEL_HEIGHT = 400
SEGMENT_DURATION = 4  # Duration of each segment in seconds, matches -hls_time 4 from encoding
SEEK_DEBOUNCE_TIME = 0.1  # Minimum time (seconds) between seek requests
DOWNLOAD_TIMEOUT_1 = 1  # First threshold (seconds) for downgrading to next quality
DOWNLOAD_TIMEOUT_2 = 3  # Second threshold (seconds) for downgrading directly to 240p
ABR_RESET_DELAY = 30  # Time (seconds) after explicit quality change to re-enable ABR
QUALITY_LEVELS = ["720p", "360p", "240p"]  # Ordered list for ABR downgrades

# Global State
downloaded_segments = set()
segment_lock = threading.Lock()
next_segment_to_play = 0
stop_flag = False
playback_state = {
    "playing": True,
    "seek_to": None,  # Represents timestamp in seconds
    "exit": False,
    "abr_enabled": True,  # Controls whether ABR can downgrade quality
    "last_explicit_quality_change": 0  # Timestamp of last user quality change
}
manifest_dict = None
current_video_name = None
current_quality = "720p"  # Start at 720p
pending_quality = None
CDN_IP = None
CDN_PORT = None
MAX_SEGMENT_INDEX = -1
TOTAL_DURATION = 0
last_seek_time = 0  # For debouncing slider seeks
last_download_time = 0  # Timestamp of last successful download
last_playback_time = 0  # Timestamp of last successful frame playback
last_not_found_log = 0  # For throttling "segment not found" logs

# Helper Functions
def get_manifest():
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(('127.0.0.1', 8080))
            s.sendall(b"GIVE MANIFEST")
            return s.recv(4096).decode()
    except Exception as e:
        print(f"[Client] Failed to get manifest: {e}")
        return None

def parse_manifest(manifest_text):
    manifest_dict = {}
    for line in manifest_text.splitlines():
        if "=>" in line:
            key, value = line.split("=>")
            key = key.strip()
            value = value.strip()
            video_quality, _ = key.split(".")
            video_name, quality = video_quality.split("_")
            if video_name not in manifest_dict:
                manifest_dict[video_name] = {}
            ip, port = value.split(":")
            manifest_dict[video_name][quality] = (ip, int(port))
    return manifest_dict

def get_segment_count(ip, port):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((ip, port))
            s.sendall(f"GET_SEGMENT_COUNT {current_video_name} {current_quality}".encode())
            return int(s.recv(1024).decode()) - 1  # Indices start from 0
    except Exception as e:
        print(f"[Client] Failed to get segment count: {e}")
        return -1

def download_segment(segment_idx, quality):
    global last_download_time
    segment_name = f"{segment_idx:03d}.ts"
    save_name = f"{quality}_{segment_name}"
    temp_path = os.path.join(BUFFER_FOLDER, f"tmp_{save_name}")
    final_path = os.path.join(BUFFER_FOLDER, save_name)
    ip, port = manifest_dict[current_video_name][quality]

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
                # Prioritize downloading the seeked segment
                seek_timestamp = playback_state["seek_to"]
                seek_segment = int(seek_timestamp // SEGMENT_DURATION)
                existing = set(os.listdir(BUFFER_FOLDER))
                if f"{quality_to_download}_{seek_segment:03d}.ts" not in existing:
                    download_segment(seek_segment, quality_to_download)
                    downloaded_segments.add(f"{quality_to_download}_{seek_segment:03d}.ts")
                next_segment_to_play = seek_segment
                playback_state["seek_to"] = None  # Reset seek after handling

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

        # Video label
        self.video_label = QLabel()
        self.video_label.setAlignment(Qt.AlignCenter)
        # self.video_label.setFixedSize(VIDEO_LABEL_WIDTH, VIDEO_LABEL_HEIGHT)
        self.video_label.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.video_label.setStyleSheet("background-color: black;")  # Black background for video area

        # Play button
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

        # Seek slider
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

        # Quality buttons
        quality_layout = QHBoxLayout()
        quality_layout.setSpacing(10)  # 10px between buttons
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

        # Control bar (play button + quality buttons)
        control_bar = QHBoxLayout()
        control_bar.setSpacing(20)  # 20px between play button and quality buttons
        control_bar.addWidget(self.play_button)
        control_bar.addStretch()  # Add stretchable space
        control_bar.addLayout(quality_layout)
        control_bar.addStretch()  # Add stretchable space on the right for spacing

        # Main layout
        layout = QVBoxLayout()
        layout.setSpacing(20)  # 20px between video and control bar
        layout.setContentsMargins(20, 20, 20, 20)  # 20px margins on all sides
        layout.addWidget(self.video_label)
        layout.addLayout(control_bar)
        layout.addWidget(self.slider)  # Slider below the control bar
        layout.setAlignment(Qt.AlignCenter)
        self.setLayout(layout)

        # Timer for playback
        self.timer = QTimer()
        self.timer.timeout.connect(self.play_next_frame)
        self.timer.start(30)

        self.current_cap = None
        self.current_segment = -1
        self.update_quality_styles()  # Initialize quality button styles
        self.set_quality(current_quality)  # Initialize with default quality (no ABR disable)

    def set_quality(self, quality, user_initiated=False):
        global pending_quality, CDN_IP, CDN_PORT, MAX_SEGMENT_INDEX, TOTAL_DURATION, current_quality
        if current_video_name not in manifest_dict or quality not in manifest_dict[current_video_name]:
            QMessageBox.warning(self, "Error", f"Quality {quality} not available for {current_video_name}")
            return
        ip, port = manifest_dict[current_video_name][quality]
        max_segments = get_segment_count(ip, port)
        if max_segments < 0:
            QMessageBox.warning(self, "Error", f"CDN for {quality} is not available")
            return
        pending_quality = quality
        CDN_IP, CDN_PORT = ip, port
        MAX_SEGMENT_INDEX = max_segments
        TOTAL_DURATION = (MAX_SEGMENT_INDEX + 1) * SEGMENT_DURATION
        self.slider.setRange(0, TOTAL_DURATION)
        if user_initiated:
            playback_state["abr_enabled"] = False  # Disable ABR only for user actions
            playback_state["last_explicit_quality_change"] = time.time()
        current_quality = quality  # Update current quality
        self.update_quality_styles()  # Update button styles after quality change
        print(f"[Client] Queued quality change to {quality}, total duration: {TOTAL_DURATION}s")

    def update_quality_styles(self):
        # Reset all quality buttons to default style
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
            return  # Ignore rapid seeks
        value = self.slider.value()
        playback_state["seek_to"] = value
        last_seek_time = current_time
        print(f"[Client] Seeking to timestamp {value}s")

    def play_next_frame(self):
        global next_segment_to_play, current_quality, pending_quality, CDN_IP, CDN_PORT, MAX_SEGMENT_INDEX, last_playback_time, last_not_found_log

        if playback_state["exit"] or not playback_state["playing"]:
            return

        # Re-enable ABR after ABR_RESET_DELAY if user explicitly changed quality
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
            pending_quality = None  # Reset pending quality on seek
            print(f"[Client] Seek to segment {next_segment_to_play} from timestamp {seek_timestamp}s")
            playback_state["seek_to"] = None  # Reset seek

        if next_segment_to_play > MAX_SEGMENT_INDEX:
            playback_state["exit"] = True
            self.close()
            return

        segment_name = f"{current_quality}_{next_segment_to_play:03d}.ts"
        segment_path = os.path.join(BUFFER_FOLDER, segment_name)

        if not os.path.exists(segment_path):
            current_time = time.time()
            if current_time - last_not_found_log >= 1:  # Log every 1s to reduce spam
                print(f"[Client] Segment {segment_path} not found, waiting for download")
                last_not_found_log = current_time
            # ABR: Downgrade based on waiting time
            if playback_state["abr_enabled"]:
                waiting_time = current_time - last_playback_time
                current_idx = QUALITY_LEVELS.index(current_quality)
                if waiting_time > DOWNLOAD_TIMEOUT_2 and current_quality != "240p":
                    # Second threshold: downgrade directly to 240p
                    pending_quality = "240p"
                    print(f"[ABR] Segment {segment_path} unavailable for {waiting_time:.2f}s (> {DOWNLOAD_TIMEOUT_2}s), downgrading directly to 240p")
                elif waiting_time > DOWNLOAD_TIMEOUT_1 and current_idx < len(QUALITY_LEVELS) - 1:
                    # First threshold: downgrade to next quality
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
                last_playback_time = time.time()  # Update to avoid immediate downgrade
                return

        if not self.current_cap or not self.current_cap.isOpened():
            print(f"[Client] Invalid video capture for segment {segment_path}")
            next_segment_to_play += 1
            last_playback_time = time.time()  # Update to avoid immediate downgrade
            return

        ret, frame = self.current_cap.read()
        if not ret or frame is None:
            self.current_cap.release()
            self.current_cap = None
            next_segment_to_play += 1
            self.slider.setValue(next_segment_to_play * SEGMENT_DURATION)
            last_playback_time = time.time()  # Update to avoid immediate downgrade
            # Switch to pending quality after segment finishes
            if pending_quality:
                current_quality = pending_quality
                pending_quality = None
                ip, port = manifest_dict[current_video_name][current_quality]
                CDN_IP, CDN_PORT = ip, port
                MAX_SEGMENT_INDEX = get_segment_count(ip, port)
                if MAX_SEGMENT_INDEX < 0:
                    QMessageBox.warning(self, "Error", f"CDN for {current_quality} is not available")
                    playback_state["exit"] = True
                    return
                TOTAL_DURATION = (MAX_SEGMENT_INDEX + 1) * SEGMENT_DURATION
                self.slider.setRange(0, TOTAL_DURATION)
                self.update_quality_styles()  # Update styles after quality switch
                print(f"[Client] Switched to quality {current_quality}, total duration: {TOTAL_DURATION}s")
            return

        last_playback_time = time.time()  # Update on successful frame
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
        print("[Client] Could not retrieve manifest. Exiting.")
        return
    manifest_dict = parse_manifest(manifest_text)

    current_video_name = input("Enter video name (e.g., 'new', 'video'): ").strip()

    if current_video_name not in manifest_dict:
        print(f"[Client] Video '{current_video_name}' not found in manifest.")
        return

    # Initialize with default quality (720p)
    if "720p" in manifest_dict[current_video_name]:
        CDN_IP, CDN_PORT = manifest_dict[current_video_name]["720p"]
        MAX_SEGMENT_INDEX = get_segment_count(CDN_IP, CDN_PORT)
        if MAX_SEGMENT_INDEX < 0:
            print("[Client] CDN for default quality (720p) not available. Exiting.")
            return
        TOTAL_DURATION = (MAX_SEGMENT_INDEX + 1) * SEGMENT_DURATION
    else:
        print("[Client] Default quality (720p) not available for this video. Exiting.")
        return

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