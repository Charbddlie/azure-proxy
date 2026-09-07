"""Prewarmed Uvicorn worker; admission is controlled on a private socketpair."""

import asyncio
import ctypes
import os
import signal
import socket

import uvicorn

from .supervisor import read_message, send_message


class ServingWorker(uvicorn.Server):
    async def startup(self, sockets=None):
        await self.lifespan.startup()
        if self.lifespan.should_exit:
            self.should_exit = True
            return
        config = self.config
        loop = asyncio.get_running_loop()
        def protocol():
            return config.http_protocol_class(config=config, server_state=self.server_state,
                                              app_state=self.lifespan.state, _loop=loop)
        self.servers = [await loop.create_server(protocol, sock=sockets[0], start_serving=False)]
        control = socket.socket(fileno=int(os.environ["AZURE_PROXY_CONTROL_FD"]))
        control.setblocking(False)
        self.reader, self.writer = await asyncio.open_connection(sock=control)
        await send_message(self.writer, dict(state="ready", pid=os.getpid()))
        self.controller = asyncio.create_task(self.commands())
        self.started = True

    async def commands(self):
        try:
            while True:
                command = await read_message(self.reader, None)
                action = command["action"]
                if action == "activate":
                    for server in self.servers:
                        await server.start_serving()
                elif action == "drain":
                    for server in self.servers:
                        server.close()
                    for connection in list(self.server_state.connections):
                        connection.shutdown()
                    self.should_exit = True
                else:
                    raise RuntimeError("invalid worker command")
                await send_message(self.writer, dict(state=action))
                if action == "drain":
                    return
        except (OSError, RuntimeError):
            self.should_exit = True

    async def shutdown(self, sockets=None):
        await super().shutdown(sockets)
        self.controller.cancel()
        await asyncio.gather(self.controller, return_exceptions=True)
        self.writer.close()


def main():
    ctypes.CDLL(None).prctl(1, signal.SIGTERM)  # No accepting orphan after supervisor death.
    if os.getppid() != int(os.environ["AZURE_PROXY_SUPERVISOR_PID"]):
        return
    async def run():
        from .server import app, cfg
        listener = socket.socket(fileno=int(os.environ["AZURE_PROXY_LISTEN_FD"]))
        config = uvicorn.Config(app, host=cfg.host, port=cfg.port, log_level=cfg.log_level)
        await ServingWorker(config).serve(sockets=[listener])
    asyncio.run(run())


if __name__ == "__main__":
    main()
