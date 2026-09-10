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
        self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
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
    """
    Edge device sends aggregated JSON here.
    Broadcasts instantly to connected dashboards via WebSocket.
    """
    payload["server_received_at"] = datetime.utcnow().isoformat()
    
    # Update state based on event type
    event_type = payload.get("event_type")
    if event_type == "QUEUE_UPDATE":
        store_state["counters"] = payload.get("payload", {}).get("counters", store_state["counters"])
    elif event_type == "INVENTORY_HEALTH":
        store_state["shelves"] = payload.get("payload", {}).get("shelves", store_state["shelves"])
        
    # Broadcast to all connected WebSockets
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
    # Send current state immediately on connection
    await websocket.send_json({"type": "INITIAL_HYDRATION", "data": store_state})
    try:
        while True:
            # Keep connection alive
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)


# Yeh background task har 2 second mein numbers update karega
async def simulate_live_edge_data():
    while True:
        await asyncio.sleep(2)
        if hasattr(manager, "active_connections") and manager.active_connections:
            # Footfall aur FPS mein live variation
            delta = random.choice([-1, 0, 1, 2])
            store_state["active_footfall"] = max(10, store_state.get("active_footfall", 42) + delta)
            store_state["fps"] = round(random.uniform(27.8, 29.6), 1)
            
            # Subscribed frontend ko live data bhejna
            payload = {"type": "STATE_UPDATE", "data": store_state}
            for connection in list(manager.active_connections):
                try:
                    await connection.send_json(payload)
                except Exception:
                    pass
                    
@app.on_event("startup")
async def startup_event():
    asyncio.create_task(simulate_live_edge_data())
