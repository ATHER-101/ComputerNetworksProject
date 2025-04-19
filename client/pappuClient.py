# client.py

import socket

def request_video(video_name, cdn_ip='127.0.0.1', cdn_port=8000):
    try:
        # Create a TCP socket and connect to the CDN's client request server.
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((cdn_ip, cdn_port))
        print(f"[Client] Connected to CDN at {cdn_ip}:{cdn_port}")

        # Encode the video name and send its length (1 byte) then the name itself.
        video_name_bytes = video_name.encode()
        s.send(len(video_name_bytes).to_bytes(1, 'big'))
        s.send(video_name_bytes)
        print(f"[Client] Requested video: {video_name}")

        # Receive the file size (8 bytes).
        file_size = int.from_bytes(s.recv(8), 'big')
        if file_size == 0:
            print(f"[Client] Video '{video_name}' is not available on CDN.")
            s.close()
            return

        print(f"[Client] Receiving video '{video_name}' ({file_size} bytes).")

        # Receive the video content in chunks.
        received_bytes = 0
        video_data = bytearray()
        while received_bytes < file_size:
            chunk = s.recv(min(4096, file_size - received_bytes))
            if not chunk:
                break
            video_data.extend(chunk)
            received_bytes += len(chunk)

        # Close the connection.
        s.close()

        # Write the received data to a file.
        local_filename = f"downloaded_{video_name}"
        with open(local_filename, "wb") as f:
            f.write(video_data)
        print(f"[Client] Video saved as '{local_filename}'.")

    except Exception as e:
        print("[Client] Error requesting video:", e)

if __name__ == "__main__":
    # Replace 'sample_video_240p.ts' with the name of an encoded video in the distribution.
    request_video("720p_000.ts")
