from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import json
import asyncio
import random
from datetime import datetime

app = FastAPI(title="EdgeRetail Analytics Gateway")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Active WebSocket connections pool (Frontend clients)
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in list(self.active_connections):
            try:
                await connection.send_json(message)
            except Exception:
                pass

manager = ConnectionManager()

# In-Memory State for Instant Dashboard Hydration
store_state = {
    "node_status": {
        "device_id": "Jetson-Orin-01",
        "fps": 28.4,
        "wifi_dbm": -62,
        "battery_pct": 100,
        "status": "LIVE"
    },
    "kpi": {
        "active_footfall": 42,
        "in_count": 318,
        "out_count": 276,
        "avg_dwell_time": "14m 20s",
        "critical_alerts": 3
    },
    "counters": [
        {"id": "01", "status": "ACTIVE", "queue": 5, "wait_time": "7m 30s", "alert": "CRITICAL_CONGESTION"},
        {"id": "02", "status": "ACTIVE", "queue": 2, "wait_time": "2m 10s", "alert": "NORMAL"},
        {"id": "03", "status": "STANDBY", "queue": 0, "wait_time": "0m", "alert": "NONE"}
    ],
    "shelves": [
        {"aisle": "Aisle 2 - Snacks", "sku": "Parle-G", "fill_pct": 30, "status": "LOW_STOCK"},
        {"aisle": "Aisle 4 - Dairy", "sku": "Amul Milk", "fill_pct": 85, "status": "OPTIMAL"},
        {"aisle": "Aisle 1 - Edible Oils", "sku": "Sunflow 1L", "fill_pct": 0, "status": "OUT_OF_STOCK"}
    ],
    "events": []
}

# 1. Edge Sync Endpoints
@app.post("/api/v1/telemetry/edge")
async def ingest_edge_telemetry(payload: dict):
    payload["server_received_at"] = datetime.utcnow().isoformat()
    
    event_type = payload.get("event_type")
    if event_type == "QUEUE_UPDATE":
        store_state["counters"] = payload.get("payload", {}).get("counters", store_state["counters"])
    elif event_type == "INVENTORY_HEALTH":
        store_state["shelves"] = payload.get("payload", {}).get("shelves", store_state["shelves"])
        
    await manager.broadcast({"type": event_type, "data": payload})
    return {"status": "ACK", "message": "Telemetry synced"}

# 2. REST Endpoint for initial state load
@app.get("/api/v1/store/live-state")
def get_live_state():
    return store_state

# 3. Real-Time WebSocket for Vercel Frontend
@app.websocket("/ws/live-stream")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    await websocket.send_json({"type": "INITIAL_HYDRATION", "data": store_state})
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

# 4. Background Telemetry Simulation Task
async def simulate_live_edge_data():
    while True:
        await asyncio.sleep(2)
        if manager.active_connections:
            # Random fluctuations match frontend structure
            delta = random.choice([-1, 0, 1, 2])
            current_footfall = store_state["kpi"]["active_footfall"]
            store_state["kpi"]["active_footfall"] = max(10, current_footfall + delta)
            
            # Fluctuate FPS & Wi-Fi in node_status
            store_state["node_status"]["fps"] = round(random.uniform(27.5, 29.8), 1)
            store_state["node_status"]["wifi_dbm"] = random.randint(-65, -58)

            # Fluctuate Queue counts
            q1 = max(1, min(8, store_state["counters"][0]["queue"] + random.choice([-1, 0, 1])))
            store_state["counters"][0]["queue"] = q1
            store_state["counters"][0]["wait_time"] = f"{q1 * 90 // 60}m {q1 * 90 % 60}s"

            # Broadcast updated state
            payload = {"type": "STATE_UPDATE", "data": store_state}
            await manager.broadcast(payload)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(simulate_live_edge_data())
