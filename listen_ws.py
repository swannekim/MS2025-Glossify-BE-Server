# listen_ws.py
import socketio, sys
mid = sys.argv[1] if len(sys.argv) > 1 else "finance-005"

sio = socketio.Client()

@sio.event
def connect():
    print("connected")
    sio.emit("join", {"meeting_id": mid})
    
@sio.on("terms")
def on_terms(data):
    print("[WS][terms]", data)

@sio.on("ack")
def on_ack(data):
    print("[WS][ack]", data)

@sio.on("error")
def on_err(data):
    print("[WS][error]", data)

@sio.on("cosmos_upsert_done")
def on_done(data):
    print("[WS] DONE:", data)

@sio.on("cosmos_upsert_error")
def on_err(data):
    print("[WS] ERROR:", data)

sio.connect("http://localhost:5000", transports=["websocket"])
sio.wait()