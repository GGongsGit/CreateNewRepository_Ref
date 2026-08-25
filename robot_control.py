"""
robot_control.py — WebSocket bridge for driving robot-arm-simulator.html from Python.

The simulator page always connects OUT to ws://localhost:8765 as a client (see the
"EXTERNAL JOINT CONTROL" section near the end of robot-arm-simulator.html), auto-
reconnecting every 2s if the server isn't up yet or drops. So the usual order is:

    1. pip install websockets
    2. Run this script (or import RobotSimServer and start() it yourself) — it opens
       the WebSocket server and waits for the browser tab to connect.
    3. Open robot-arm-simulator.html in a browser (or it's already open) — the "🔌"
       badge in the top bar turns green once connected.

Joint values are plain degrees, sent in the order the simulator's joint panel shows
them (JOINTS[0..N-1]) — this matches whichever robot is currently loaded (ARM-6, or
any saved model, 6-axis or otherwise). Values outside a joint's limit are clamped by
the simulator itself, same as dragging a slider past its end.

Example:
    from robot_control import RobotSimServer

    sim = RobotSimServer()
    sim.start()
    sim.wait_for_connection()
    sim.send_joints([-35.8, 42.7, -5.3, -182.1, -90.2, -17.9], animate=True)
    print(sim.get_state())
"""

###  ENV - comPy313Env

import asyncio
import json
import threading
import time
from typing import Optional, Sequence

import websockets


class RobotSimServer:
    def __init__(self, host: str = "localhost", port: int = 8765):
        self.host = host
        self.port = port
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._clients = set()
        self._thread: Optional[threading.Thread] = None
        self._ready = threading.Event()
        self._next_id = 0
        self._pending = {}  # id -> (threading.Event, result box), for get_state() replies

    # ---- lifecycle ----
    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        if not self._ready.wait(timeout=5):
            raise RuntimeError("WebSocket server failed to start")
        print(f"[robot_control] listening on ws://{self.host}:{self.port} "
              f"— open robot-arm-simulator.html and it will auto-connect")

    def stop(self):
        if self._loop:
            self._loop.call_soon_threadsafe(self._loop.stop)

    def wait_for_connection(self, timeout: Optional[float] = None):
        """Blocks until the simulator's browser tab has connected."""
        start = time.time()
        while not self.connected:
            if timeout is not None and (time.time() - start) > timeout:
                raise TimeoutError("No simulator browser tab connected in time")
            time.sleep(0.1)

    @property
    def connected(self) -> bool:
        return len(self._clients) > 0

    # ---- internals ----
    def _run_loop(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._serve())
        self._ready.set()
        self._loop.run_forever()

    async def _serve(self):
        await websockets.serve(self._handler, self.host, self.port)

    async def _handler(self, ws):
        self._clients.add(ws)
        try:
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                mid = msg.get("id")
                if mid is not None and mid in self._pending:
                    ev, box = self._pending.pop(mid)
                    box["result"] = msg
                    ev.set()
        finally:
            self._clients.discard(ws)

    def _send(self, payload: dict):
        if not self._loop:
            raise RuntimeError("Server not started — call start() first")
        data = json.dumps(payload)
        asyncio.run_coroutine_threadsafe(self._broadcast(data), self._loop)

    async def _broadcast(self, data: str):
        if not self._clients:
            return
        await asyncio.gather(*[c.send(data) for c in list(self._clients)], return_exceptions=True)

    def _send_and_wait(self, payload: dict, timeout: float) -> dict:
        mid = self._next_id
        self._next_id += 1
        payload["id"] = mid
        ev = threading.Event()
        box = {}
        self._pending[mid] = (ev, box)
        self._send(payload)
        if not ev.wait(timeout=timeout):
            self._pending.pop(mid, None)
            raise TimeoutError("No response from simulator (is a browser tab connected?)")
        return box["result"]

    # ---- public control API ----
    def send_joints(self, values: Sequence[float], animate: bool = False):
        """Send joint angles in degrees, simulator's current joint order.
        animate=True eases smoothly over ~0.9s (like the Demo Run button);
        animate=False (default) snaps immediately — better for tight control loops."""
        self._send({"type": "joints", "values": list(values), "animate": animate})

    def get_state(self, timeout: float = 2.0) -> dict:
        """Blocks for the simulator's current joint values + TCP pose (mm/deg).
        Returns {"joints": [{"id","name","val","min","max"}, ...], "tcp": {...}}."""
        reply = self._send_and_wait({"type": "get_state"}, timeout)
        return {"joints": reply.get("joints"), "tcp": reply.get("tcp")}


if __name__ == "__main__":
    sim = RobotSimServer()
    sim.start()
    print("Waiting for robot-arm-simulator.html to connect...")
    sim.wait_for_connection()
    print("Connected. Running a short demo motion.")
    while sim.connected:
        sim.send_joints([-35.8, 42.7, -5.3, -182.1, -90.2, -17.9], animate=True)
        time.sleep(1.5)
        print("Current state:", json.dumps(sim.get_state(), indent=2, ensure_ascii=False))

        time.sleep(1)
        sim.send_joints([0, 0, 0, 0, 0, 0], animate=True)
        time.sleep(1.5)

        print("Demo done. Server keeps running — Ctrl+C to exit.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
