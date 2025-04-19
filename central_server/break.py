import socket
import subprocess
import json
import os

# Function to get video duration using ffprobe
def get_video_duration(file_path):
    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v", "error",
                "-show_entries", "format=duration",
                "-of", "json",
                file_path
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        data = json.loads(result.stdout)
        duration = float(data["format"]["duration"])
        return duration
    except Exception as e:
        print(f"Error getting duration of {file_path}: {e}")
        return None

# Function to split a .ts file into two parts
def split_ts_file(input_file, output_partA, output_partB):
    duration = get_video_duration(input_file)
    if duration is not None:
        half_duration = duration / 2
        subprocess.run([
            "ffmpeg", "-i", input_file,
            "-ss", "0", "-t", str(half_duration), "-c", "copy", output_partA
        ])
        subprocess.run([
            "ffmpeg", "-i", input_file,
            "-ss", str(half_duration), "-c", "copy", output_partB
        ])
        print(f"Split {input_file} into {output_partA} and {output_partB} at {half_duration:.2f} seconds")
        return True
    else:
        print(f"Skipping {input_file} due to duration retrieval failure")
        return False

# Device configuration
devices = {
    "device1": ("127.0.0.1", 5000),
    "device2": ("127.0.0.1", 5000),
    "device3": ("127.0.0.1", 5000),
    "device4": ("127.0.0.1", 5000),
    "device5": ("127.0.0.1", 5000),
}

# Step 1: Process all .ts files in the folder
folder_path = "./encoded_videos/20250417-001950-b0fd92"
manifest_file = "manifest.txt"

# Clear or create the manifest file
with open(manifest_file, "w") as f:
    f.write("Manifest of video parts distribution\n")

for filename in os.listdir(folder_path):
    if filename.endswith(".ts"):
        input_file = os.path.join(folder_path, filename)
        print(input_file)
        
        # Define output filenames (e.g., video_500k_partA.ts, video_500k_partB.ts)
        base_name = os.path.splitext(filename)[0]
        partA = f"{base_name}_partA.ts"
        partB = f"{base_name}_partB.ts"

        # Split the file
        if split_ts_file(input_file, partA, partB):
            # Step 3: Send parts to devices
            parts = {
                partA: ["device1", "device2", "device3"],
                partB: ["device4", "device5"],
            }

            for part, dev_list in parts.items():
                # Read the part file
                with open(part, "rb") as f:
                    file_data = f.read()

                # Send to each device in the list
                for dev in dev_list:
                    ip, port = devices[dev]
                    try:
                        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        print("hello")
                        print(devices["device1"])
                        s.connect((ip, port))
                        print("hello")
                        s.sendall(part.encode() + b"\n")  # Send part name
                        s.sendall(file_data)  # Send file content
                        print(f"Sent {part} to {dev} ({ip}:{port})")
                    except Exception as e:
                        print(f"Error sending {part} to {dev}: {e}")
                    finally:
                        s.close()

            # Optional: Clean up split files after sending
            # os.remove(partA)
            # os.remove(partB)