"""Per-role process ownership, including compatibility with the old pidfile."""

import os
from .state import InstanceLock


class ProcessClaim:
    def __init__(self, root, role):
        self.lock = InstanceLock(root, role)
        self.path = os.path.join(root, ".proxy.pid" if role == "serving" else ".routing.pid")
        try:
            with open(self.path) as file:
                previous = int(file.read().strip())
            if previous > 0 and previous != os.getpid():
                try:
                    os.kill(previous, 0)
                    with open("/proc/{}/stat".format(previous)) as status:
                        zombie = status.read().rpartition(")")[2].split()[0] == "Z"
                except ProcessLookupError:
                    pass
                except FileNotFoundError:
                    pass
                else:
                    if not zombie:
                        raise RuntimeError("{} pid {} is still running".format(role, previous))
        except (FileNotFoundError, ValueError):
            pass
        with open(self.path, "w") as file:
            file.write(str(os.getpid()) + "\n")

    def close(self):
        try:
            with open(self.path) as file:
                owned = file.read().strip() == str(os.getpid())
            if owned:
                os.unlink(self.path)
        except FileNotFoundError:
            pass
        self.lock.close()
