from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMSMiddleware
from datetime import datetime
import asyncio
import random

app = FastAPI(title="EdgeRetail OS Gateway")

app.add_middleware(
    CORSMSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global Store State
store_state = {
    "node_status": {
        "device_id": "Jetson-Orin-01",
        "status": "ONLINE",
        "fps": 5.5,
        "wifi_rssi": -61,
        "mode": "LIVE"
    },
    "kpi": {
        "active_footfall": 1,
        "in_count": 0,
        "out_count": 0,
        "avg_dwell_time": "12m 40s",
        "restock_alerts": 1
    },
    "counters": [
        {"id": "c1", "name": "COUNTER 01", "queue": 1, "wait_time": "1m 30s", "status": "ACTIVE"},
        {"id": "c2", "name": "COUNTER 02", "queue": 0, "wait_time": "0m 00s", "status": "STANDBY"},
        {"id": "c3", "name": "COUNTER 03", "queue": 0, "wait_time": "0m 00s", "status": "STANDBY"}
    ],
    "inventory": [
        {"sku": "SKU-9921", "name": "Organic Almond Milk", "stock": 42, "threshold": 15, "status": "OPTIMAL"},
        {"sku": "SKU-4412", "name": "Whole Wheat Bread", "stock": 8, "threshold": 12, "status": "LOW_STOCK"},
        {"sku": "SKU-1089", "name": "Greek Yogurt 500g", "stock": 3, "threshold": 10, "status": "CRITICAL"}
    ]
}

class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                pass

manager = ConnectionManager()

# 1. Edge Sync Endpoint (Directly updates store state from edge_tracker.py)
@app.post("/api/v1/telemetry/edge")
async def ingest_edge_telemetry(payload: dict):
    # Sync FPS
    if "fps" in payload:
        store_state["node_status"]["fps"] = payload["fps"]

    # Sync Live Counts
    if "footfall_metrics" in payload:
        m = payload["footfall_metrics"]
        store_state["kpi"]["active_footfall"] = m.get("active_in_frame", store_state["kpi"]["active_footfall"])
        store_state["kpi"]["in_count"] = m.get("in_count", store_state["kpi"]["in_count"])
        store_state["kpi"]["out_count"] = m.get("out_count", store_state["kpi"]["out_count"])

        # Dynamic Queue mapping with active people detected
        active = m.get("active_in_frame", 0)
        store_state["counters"][0]["queue"] = active
        store_state["counters"][0]["wait_time"] = f"{active * 45}s"
        
        # If queue >= 3, trigger alert mode
        if active >= 3:
            store_state["counters"][0]["status"] = "CONGESTED"
        else:
            store_state["counters"][0]["status"] = "ACTIVE"

    # Dynamic Inventory Consumption when IN count increases
    if payload.get("trigger_inventory_decrement", False):
        for item in store_state["inventory"]:
            if item["stock"] > 0 and random.random() > 0.4:
                item["stock"] -= 1
                if item["stock"] <= item["threshold"] and item["stock"] > 5:
                    item["status"] = "LOW_STOCK"
                elif item["stock"] <= 5:
                    item["status"] = "CRITICAL"
        
        # Count restock alerts
        critical_count = sum(1 for item in store_state["inventory"] if item["status"] in ["LOW_STOCK", "CRITICAL"])
        store_state["kpi"]["restock_alerts"] = critical_count

    # Broadcast updated state immediately to Next.js
    await manager.broadcast({"type": "STATE_UPDATE", "data": store_state})
    return {"status": "ACK", "synced": True}

# 2. WebSocket for Next.js Dashboard
@app.websocket("/ws/live-stream")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    # Send current state on connection
    await websocket.send_json({"type": "INIT", "data": store_state})
    try:
        while True:
            data = await websocket.receive_json()
            # Handle user actions from Dashboard (e.g. OPEN NEXT COUNTER)
            if data.get("action") == "OPEN_COUNTER":
                counter_id = data.get("counter_id", "c2")
                for c in store_state["counters"]:
                    if c["id"] == counter_id:
                        c["status"] = "ACTIVE"
                await manager.broadcast({"type": "STATE_UPDATE", "data": store_state})
    except WebSocketDisconnect:
        manager.disconnect(websocket)
