"""Socket-owning supervisor with readiness-gated, serialized rolling switches."""

import asyncio
import json
import logging
import os
import signal
import socket
import sys
import time

from .config import Config, ROOT
from .process import ProcessClaim


def control_path(root=ROOT):
    return os.path.join(root, "runtime", "supervisor.sock")


def status_path(root=ROOT):
    return os.path.join(root, "runtime", "supervisor.json")


async def read_message(reader, timeout=30):
    line = await asyncio.wait_for(reader.readline(), timeout)
    if not line:
        raise RuntimeError("worker readiness channel closed")
    return json.loads(line)


async def send_message(writer, message):
    writer.write(json.dumps(message).encode() + b"\n")
    await writer.drain()


class Worker:
    async def command(self, action):
        await send_message(self.writer, dict(action=action))
        result = await read_message(self.reader, 5)
        if result.get("state") != action:
            raise RuntimeError("worker did not acknowledge " + action)

    async def terminate(self):
        if self.process.returncode is None:
            self.process.kill()
        await self.process.wait()
        self.writer.close()


class Supervisor:
    def __init__(self):
        self.cfg = Config(load_routes=False)
        self.active = self.starting = None
        self.draining = []
        self.switching = asyncio.Lock()
        self.stop = asyncio.Event()
        self.last_switch = self.last_error = None
        self.listener = None
        self.log_handler = None
        if os.environ.get("AZURE_PROXY_MANAGED_LOG"):
            from .logfiles import handler, release_bootstrap_stdio
            self.log_handler = handler(ROOT, "serving")
            release_bootstrap_stdio()

    async def copy_log(self, reader):
        while True:
            line = await reader.readline()
            if not line:
                return
            record = logging.LogRecord("serving", logging.INFO, "", 0,
                                       line.decode("utf-8", "replace").rstrip("\n"), (), None)
            await asyncio.to_thread(self.log_handler.emit, record)

    def publish(self):
        state = dict(pid=os.getpid(), heartbeat=time.time(),
                     active=self.active.process.pid if self.active else None,
                     starting=self.starting.process.pid if self.starting else None,
                     draining=[w.process.pid for w in self.draining if w.process.returncode is None],
                     last_switch=self.last_switch, error=self.last_error)
        path = status_path()
        fd = os.open(path + ".tmp", os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o600)
        with os.fdopen(fd, "w") as file:
            json.dump(state, file)
        os.replace(path + ".tmp", path)

    async def launch(self, timeout):
        worker = Worker()
        parent, child = socket.socketpair()
        parent.setblocking(False)
        started = time.monotonic()
        try:
            worker.process = await asyncio.create_subprocess_exec(
                sys.executable, "-m", "proxy.worker",
                env=dict(os.environ, AZURE_PROXY_LISTEN_FD=str(self.listener.fileno()),
                         AZURE_PROXY_CONTROL_FD=str(child.fileno()),
                         AZURE_PROXY_SUPERVISOR_PID=str(os.getpid())),
                pass_fds=(self.listener.fileno(), child.fileno()),
                stdout=asyncio.subprocess.PIPE if self.log_handler else None,
                stderr=asyncio.subprocess.STDOUT if self.log_handler else None,
                limit=1024 * 1024)
        finally:
            child.close()
        worker.reader, worker.writer = await asyncio.open_connection(sock=parent)
        if self.log_handler:
            worker.log_task = asyncio.create_task(self.copy_log(worker.process.stdout))
        self.starting = worker
        self.publish()
        try:
            ready = await read_message(worker.reader, timeout)
            if ready.get("state") != "ready":
                raise RuntimeError("worker warmup failed")
        except BaseException:
            await worker.terminate()
            self.starting = None
            raise
        worker.warmup_seconds = time.monotonic() - started
        return worker

    async def roll(self, timeout=30):
        self.draining = [w for w in self.draining if w.process.returncode is None]
        if self.switching.locked() or self.draining:
            raise RuntimeError("a worker is already starting or draining")
        async with self.switching:
            worker = None
            try:
                worker = await self.launch(timeout)
                switch_at = time.monotonic()
                await worker.command("activate")
                old = self.active
                self.active, self.starting = worker, None
                if old:
                    self.draining.append(old)
                    try:
                        await old.command("drain")
                    except (RuntimeError, OSError, asyncio.TimeoutError):
                        if old.process.returncode is None:
                            old.process.terminate()
                self.last_switch = dict(warmup_seconds=worker.warmup_seconds,
                                        switch_seconds=time.monotonic() - switch_at,
                                        active=worker.process.pid)
                self.last_error = None
                self.publish()
                return self.last_switch
            except BaseException as exc:
                if worker and worker is not self.active:
                    await worker.terminate()
                self.starting = None
                self.last_error = str(exc) or type(exc).__name__
                self.publish()
                raise

    async def control(self, reader, writer):
        try:
            command = await read_message(reader, 5)
            if command.get("action") != "restart":
                raise RuntimeError("unknown supervisor command")
            result = await self.roll(float(command.get("timeout", 30)))
            await send_message(writer, dict(ok=True, **result))
        except Exception as exc:
            try:
                await send_message(writer, dict(ok=False, error=str(exc) or type(exc).__name__))
            except OSError:
                pass
        finally:
            writer.close()

    async def run(self):
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self.stop.set)
        self.listener = socket.socket(socket.AF_INET6 if ":" in self.cfg.host else socket.AF_INET)
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind((self.cfg.host, self.cfg.port))
        self.listener.listen(2048)
        self.listener.setblocking(False)
        path = control_path()
        if os.path.exists(path):
            os.unlink(path)  # Protected by the serving ProcessClaim.
        control = await asyncio.start_unix_server(self.control, path)
        os.chmod(path, 0o600)
        try:
            await self.roll()
            while not self.stop.is_set():
                self.draining = [w for w in self.draining if w.process.returncode is None]
                if self.active and self.active.process.returncode is not None:
                    self.last_error = "active worker exited"
                    self.active = None
                self.publish()
                try:
                    await asyncio.wait_for(self.stop.wait(), 0.25)
                except asyncio.TimeoutError:
                    pass
        finally:
            control.close()
            await control.wait_closed()
            async with self.switching:
                workers = self.draining + [w for w in (self.active, self.starting) if w]
                for worker in workers:
                    if worker.process.returncode is None:
                        worker.process.terminate()
                await asyncio.gather(*(w.process.wait() for w in workers))
            self.active = self.starting = None
            self.draining = []
            self.publish()
            self.listener.close()
            os.unlink(path)
            if self.log_handler:
                self.log_handler.close()


def main():
    claim = ProcessClaim(ROOT, "serving")
    async def run():
        supervisor = Supervisor()
        try:
            await supervisor.run()
        except Exception:
            if supervisor.log_handler:
                import traceback
                supervisor.log_handler.emit(logging.LogRecord(
                    "supervisor", logging.ERROR, "", 0, traceback.format_exc(), (), None))
            raise
    try:
        asyncio.run(run())
    finally:
        claim.close()
