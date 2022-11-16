import time
import sys
from utils.websocket_client import WebsocketClient

def run():
    wsClient = WebsocketClient()
    wsClient.start()
    try:
        while True:
            print("\nMessageCount =", "%i \n" % wsClient.message_count)
            time.sleep(1)
    except KeyboardInterrupt:
        wsClient.close()

    if wsClient.error:
        sys.exit(1)
    else:
        sys.exit(0)

if __name__ == "__main__":
    run()