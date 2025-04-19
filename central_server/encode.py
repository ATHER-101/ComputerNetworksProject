import os
import subprocess

# Configuration
ENCODED_DIR = "encoded_videos"
BITRATES = [
    {"height": 240, "crf": 28, "bitrate": "500000"},
    {"height": 360, "crf": 23, "bitrate": "1000000"}, 
    {"height": 720, "crf": 20, "bitrate": "2500000"}
]

def encode_video(input_path):
    try:
        filename = os.path.splitext(os.path.basename(input_path))[0]
        output_dir = os.path.join(ENCODED_DIR, filename)
        os.makedirs(output_dir, exist_ok=True)

        for br in BITRATES:
            output_template = os.path.join(output_dir, f"{br['height']}p")

            ffmpeg_cmd = [
                "ffmpeg",
                "-i", input_path,
                "-vf", f"scale=-2:{br['height']}",
                "-c:v", "libx264",
                "-crf", str(br['crf']),
                "-c:a", "aac",
                "-b:a", "128k",
                "-hls_time", "4",
                "-hls_playlist_type", "vod",
                "-hls_segment_filename", f"{output_template}_%03d.ts",
                f"{output_template}.m3u8"
            ]

            subprocess.run(ffmpeg_cmd, check=True)

        # Generate master playlist
        master_path = os.path.join(output_dir, "master.m3u8")
        with open(master_path, "w") as f:
            f.write("#EXTM3U\n#EXT-X-VERSION:3\n")
            for br in BITRATES:
                f.write(f"#EXT-X-STREAM-INF:BANDWIDTH={br['bitrate']},RESOLUTION=1280x{br['height']}\n")
                f.write(f"{br['height']}p.m3u8\n")

        print(f"Encoding complete for {filename}")

    except subprocess.CalledProcessError as e:
        print(f"Encoding failed: {e}")