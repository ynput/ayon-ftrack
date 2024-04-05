import sys
import signal
import socket

from ayon_core.lib import Logger
from ayon_core.addon import AddonsManager

from ayon_ftrack.common import FtrackServer

from ayon_ftrack.tray.user_server import (
    SocketSession,
    SocketBaseEventHub
)

log = Logger.get_logger("FtrackUserServer")


def main(args):
    port = int(args[-1])

    # Create a TCP/IP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Connect the socket to the port where the server is listening
    server_address = ("localhost", port)
    log.debug(
        "User Ftrack Server connected to {} port {}".format(*server_address)
    )
    try:
        sock.connect(server_address)
        sock.sendall(b"CreatedUser")
    except OSError:
        log.error(f"Failed to create connection to server {server_address}")
        return 1

    try:
        session = SocketSession(
            auto_connect_event_hub=True, sock=sock, Eventhub=SocketBaseEventHub
        )
        manager = AddonsManager()
        addon = manager.get("ftrack")
        server = FtrackServer(addon.user_event_handlers_paths)
        log.debug("Launching User Ftrack Server")
        server.run_server(session=session)
        return 0

    except Exception:
        log.warning("Ftrack session server failed.", exc_info=True)
        return 1

    finally:
        log.debug("Closing socket")
        sock.close()


if __name__ == "__main__":
    Logger.set_process_name("Ftrack User server")

    # Register interupt signal
    def signal_handler(sig, frame):
        log.info(
            "Process was forced to stop. Process ended."
        )
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    sys.exit(main(sys.argv))
