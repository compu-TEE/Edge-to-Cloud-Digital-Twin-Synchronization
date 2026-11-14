import socket
import threading

HOST = "0.0.0.0"
PORT = 9094
CONTROL_PORT = 9095
KAFKA_HOST = "kafka"
KAFKA_PORT = 9096

enabled = True
lock = threading.Lock()
active_connections = set()


def close_connection(sock):
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    try:
        sock.close()
    except OSError:
        pass


def pipe(source, destination):
    try:
        while True:
            data = source.recv(65536)
            if not data:
                break
            destination.sendall(data)
    except OSError:
        pass
    finally:
        with lock:
            pair = next(
                (p for p in active_connections if source in p or destination in p),
                None,
            )
            if pair:
                active_connections.discard(pair)
        close_connection(source)
        close_connection(destination)


def handle(client):
    global enabled

    with lock:
        if not enabled:
            client.close()
            print("Connection rejected: proxy OFF", flush=True)
            return

    try:
        upstream = socket.create_connection((KAFKA_HOST, KAFKA_PORT), timeout=5)
        client.settimeout(None)
        upstream.settimeout(None)

        pair = (client, upstream)
        with lock:
            if not enabled:
                close_connection(client)
                close_connection(upstream)
                return
            active_connections.add(pair)

        print("Connection forwarded to Kafka", flush=True)
        threading.Thread(target=pipe, args=(client, upstream), daemon=True).start()
        threading.Thread(target=pipe, args=(upstream, client), daemon=True).start()

    except OSError as exc:
        print(f"Proxy connection error: {exc}", flush=True)
        close_connection(client)


def control_server():
    global enabled

    server = socket.socket()
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", CONTROL_PORT))
    server.listen()

    while True:
        conn, _ = server.accept()
        with conn:
            command = conn.recv(100).decode().strip().upper()

            with lock:
                if command == "OFF":
                    enabled = False
                    pairs = list(active_connections)
                    active_connections.clear()
                    response = f"Proxy OFF; closing {len(pairs)} active connection(s)"
                elif command == "ON":
                    enabled = True
                    pairs = []
                    response = "Proxy ON"
                elif command == "STATUS":
                    pairs = []
                    response = f"Proxy enabled={enabled}; active connections={len(active_connections)}"
                else:
                    pairs = []
                    response = "Use ON, OFF, or STATUS"

            for pair in pairs:
                for sock in pair:
                    close_connection(sock)

            print(response, flush=True)
            conn.sendall(response.encode())


def main():
    threading.Thread(target=control_server, daemon=True).start()

    server = socket.socket()
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen()

    print(f"Kafka proxy listening on {PORT}", flush=True)

    while True:
        client, _ = server.accept()
        threading.Thread(target=handle, args=(client,), daemon=True).start()


if __name__ == "__main__":
    main()
