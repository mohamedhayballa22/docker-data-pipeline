import asyncio
from fastapi import WebSocket, WebSocketDisconnect
from typing import List, Dict, Any
from logger.logger import get_logger

logger = get_logger("websockets")

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.loop: asyncio.AbstractEventLoop | None = None

    def set_loop(self, loop: asyncio.AbstractEventLoop):
        """Set the event loop for running coroutines from threads."""
        self.loop = loop
        logger.info("WebSocket ConnectionManager event loop set.")

    async def connect(self, websocket: WebSocket):
        """Accepts a new WebSocket connection and adds it to the list."""
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"WebSocket connection accepted: {websocket.client}")

    def disconnect(self, websocket: WebSocket):
        """Removes a WebSocket connection from the list."""
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            logger.info(f"WebSocket connection closed: {websocket.client}")
        else:
            logger.debug(f"Attempted to disconnect inactive WebSocket: {websocket.client}")


    async def send_personal_message(self, message: dict, websocket: WebSocket):
        """Sends a JSON message to a specific websocket client."""
        try:
            await websocket.send_json(message)
            logger.debug(f"Sent personal message to {websocket.client}: {message.get('type', 'Unknown type')}")
        except WebSocketDisconnect:
             logger.warning(f"Client {websocket.client} disconnected before message could be sent.")
             self.disconnect(websocket)
        except RuntimeError as e:
             logger.warning(f"Runtime error sending message to {websocket.client}: {e}. Disconnecting.")
             self.disconnect(websocket)
        except Exception as e:
            logger.error(f"Unexpected error sending personal message to {websocket.client}: {e}")


    async def broadcast(self, message: dict):
        """Broadcasts a JSON message to all connected clients."""
        if not self.active_connections:
            return # No one to broadcast to

        logger.debug(f"Broadcasting message to {len(self.active_connections)} client(s): {message.get('type', 'Unknown type')}")
        connections_to_broadcast = list(self.active_connections)
        tasks = [self.send_personal_message(message, websocket) for websocket in connections_to_broadcast]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for i, result in enumerate(results):
            if isinstance(result, Exception) and not isinstance(result, WebSocketDisconnect):
                 client = connections_to_broadcast[i].client if i < len(connections_to_broadcast) else "Unknown"
                 logger.warning(f"Error during broadcast message to client {client}: {result}")


    async def send_initial_state(self, websocket: WebSocket, current_statuses: Dict[str, Any]):
        """
        Sends the current snapshot of job statuses to a newly connected client.
        The status data is passed as an argument.
        """
        logger.info(f"Sending initial state ({len(current_statuses)} jobs) to {websocket.client}")
        await self.send_personal_message({"type": "initial_state", "jobs": current_statuses}, websocket)