import socket
import time
import random


def generate_socket_data():
    # Connect to the socket server
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("localhost", 9000))

    names = ["Alice", "Bob", "Charlie", "Diana", "Eve"]

    try:
        for i in range(100):  # Generate 100 entries
            name = random.choice(names)
            value = round(random.uniform(1.0, 100.0), 1)
            data = f"{name},{value}\n"

            print(f"Sending: {data.strip()}")
            sock.send(data.encode())

            # Wait between 0.5 and 2 seconds
            time.sleep(0.5)

    except KeyboardInterrupt:
        print("\nStopping data generation...")
    finally:
        sock.close()


if __name__ == "__main__":
    generate_socket_data()
