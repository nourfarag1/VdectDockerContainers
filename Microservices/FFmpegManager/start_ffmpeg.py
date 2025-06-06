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
        "ffmpeg", "-rtsp_transport", "tcp", "-i", rtsp_url,
        "-vcodec", "copy", "-acodec", "aac", "-f", "flv",
        rtmp_url
    ]

    try:
        proc = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
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
