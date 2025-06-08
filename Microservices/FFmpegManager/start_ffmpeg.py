from flask import Flask, request, jsonify
import subprocess

app = Flask(__name__)
processes = {}

@app.route('/start-stream', methods=['POST'])
def start_stream():
    data = request.json
    rtsp_url = data.get('rtsp_url')
    stream_key = data.get('stream_key')

    if not rtsp_url or not stream_key:
        return jsonify({"error": "Missing rtsp_url or stream_key"}), 400

    # Full RTMP URL to SRS
    rtmp_url = f"rtmp://srs:1935/live/{stream_key}"
    command = [
        "ffmpeg",
        # Input options - tuned for real-time streaming and handling faulty timestamps
        "-rtsp_transport", "tcp",
        "-use_wallclock_as_timestamps", "1", # Ignore faulty RTSP timestamps and use system clock
        "-fflags", "nobuffer",
        "-analyzeduration", "1M",
        "-probesize", "1M",
        "-i", rtsp_url,

        # Output options
        "-c:v", "copy",             # Modern syntax for copying video
        "-c:a", "aac",              # Modern syntax for audio codec
        "-af", "apad",              # Pad audio to prevent it from ending early
        "-f", "flv",
        rtmp_url
    ]

    try:
        # Let FFmpeg's output go to the container logs for debugging
        proc = subprocess.Popen(command)
        processes[stream_key] = proc
        return jsonify({"message": "Stream started", "pid": proc.pid}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/stop-stream', methods=['POST'])
def stop_stream():
    data = request.json
    stream_key = data.get('stream_key')

    proc = processes.get(stream_key)
    if not proc:
        return jsonify({"error": "No such stream"}), 404

    proc.terminate()
    del processes[stream_key]
    return jsonify({"message": "Stream stopped"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
