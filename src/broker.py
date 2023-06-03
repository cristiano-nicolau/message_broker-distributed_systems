"""Message Broker"""
import enum
from typing import Dict, List, Any, Tuple

import socket, json, pickle, selectors
import xml.etree.ElementTree as ET

class Serializer(enum.Enum):
    """Possible message serializers."""

    JSON = 0
    XML = 1
    PICKLE = 2


class Broker:
    """Implementation of a PubSub Message Broker."""

    def __init__(self):
        """Initialize broker."""
        self.canceled = False
        self.topics = {}
        self.subscribers = {}
        self.sockets = {}

    def list_topics(self) -> List[str]:
        """Returns a list of strings containing all topics containing values."""
        return [topic for topic, messages in self.topics.items() if messages]

    def get_topic(self, topic: str):
        """Returns the currently stored value in topic."""
        if topic in self.topics and self.topics[topic]:
            return self.topics[topic][-1]
        return None

    def put_topic(self, topic: str, value: Any):
        """Store in topic the value."""
        if topic not in self.topics:
            self.topics[topic] = [value]
        else:
            self.topics[topic].append(value)

    def list_subscriptions(self, topic: str) -> List[Tuple[socket.socket, Serializer]]:
        """Provide list of subscribers to a given topic."""
        return [subscriber for subscriber, topics in self.subscribers.items() if topic in topics]

    def subscribe(self, topic: str, address: socket.socket, _format: Serializer = None):
        """Subscribe to topic by client in address."""
        if (address, _format) not in self.subscribers:
            self.subscribers[(address, _format)] = [topic]
        elif topic not in self.subscribers[(address, _format)]:
            self.subscribers[(address, _format)].append(topic)

    def unsubscribe(self, topic: str, address: socket.socket):
        """Unsubscribe to topic by client in address."""
        if (address, _format) in self.subscribers and topic in self.subscribers[(address, _format)]:
            self.subscribers[(address, _format)].remove(topic)

    def encode(self, method: str, topic: str, value: Any, serialize: Serializer):
        """Encode data to be sent."""
        if serialize == Serializer.JSON:
            if topic is None:
                msg = json.dumps({"method": method, "value": value}).encode("utf-8")
            else:
                msg = json.dumps({"method": method, "topic": topic, "value": value}).encode("utf-8")
            return msg

        elif serialize == Serializer.XML:
            msg = {"method": method, "topic": topic, "value": value}
            if topic is None:
                msg = ('<?xml version="1.0"?><data method="%(method)s"><value>%(value)s</value></data>' % msg)
            else:
                msg = ('<?xml version="1.0"?><data method="%(method)s" topic="%(topic)s"><value>%(value)s</value></data>' % msg)
            msg = msg.encode('utf-8')
            return msg

        elif serialize == Serializer.PICKLE:
            if topic is None:
                msg = pickle.dumps({"method": method, "value": value})
            else:
                msg = pickle.dumps({"method": method, "topic": topic, "value": value})
            return msg

    def decode(self, data: bytes, serialize: Serializer):
        """Decode data received."""
        if serialize == Serializer.JSON:
            return json.loads(data.decode('utf-8'))
        elif serialize == Serializer.XML:
            root = ET.fromstring(data.decode('utf-8'))
            msg = {}
            for child in root:
                msg[child.tag] = child.text
            return msg
        elif serialize == Serializer.PICKLE:
            return pickle.loads(data)

    def handle_publish(self, topic: str, value: Any, serialize: Serializer):
        """Handle the PUBLISH method."""
        self.put_topic(topic, value)
        for subscriber, subscribed_topics in self.subscribers.items():
            subscriber_socket, subscriber_format = subscriber
            if topic in subscribed_topics:
                msg = self.encode("PUBLISH_REP", topic, value, serialize)
                subscriber_socket.send(len(msg).to_bytes(2, 'big') + msg)

    def handle_subscribe(self, topic: str, conn: socket.socket, serialize: Serializer):
        """Handle the SUBSCRIBE method."""
        self.subscribe(topic, conn)
        final_topic = self.get_topic(topic)
        if final_topic is not None and topic is not None:
            msg = self.encode("SUBSCRIBE_REP", topic, final_topic, serialize)
            conn.send(len(msg).to_bytes(2, 'big') + msg)

    def handle_cancel(self, topic: str, conn: socket.socket):
        """Handle the CANCEL method."""
        self.unsubscribe(topic, conn)

    def handle_list_topics(self, topic: str, conn: socket.socket, serialize: Serializer):
        """Handle the LIST_TOPICS method."""
        topics = self.list_topics()
        msg = self.encode("LIST_TOPICS_REP", topic, topics, serialize)
        conn.send(len(msg).to_bytes(2, 'big') + msg)

    def handle_request(self, conn: socket.socket, mask):
        """Handle incoming requests."""
        header = conn.recv(2)
        header = int.from_bytes(header, 'big')
        length = conn.recv(header)

        if len(length) != 0:
            if conn in self.sockets:
                serialize = self.sockets[conn]
            else:
                serialize = Serializer.JSON
            data = self.decode(length, serialize)
        else:
            data = None

        if data is not None:
            if conn not in self.sockets:
                self.sockets[conn] = data["value"]

            method = data["method"]
            value = data.get("value")
            topic = data.get("topic")

            if method == "PUBLISH":
                self.handle_publish(topic, value, serialize)
            elif method == "SUBSCRIBE":
                self.handle_subscribe(topic, conn, serialize)
            elif method == "CANCEL":
                self.handle_cancel(topic, conn)
            elif method == "LIST_TOPICS":
                self.handle_list_topics(topic, conn, serialize)
        else:
            print("Closing connection", conn)
            self.sockets.pop(conn)
            self.subscribers = {subscriber: topics for subscriber, topics in self.subscribers.items() if
                                subscriber[0] != conn}

    def run(self):
        """Run until canceled."""
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind(("localhost", 5000))
        self.sock.listen()

        self.sockets = {self.sock: Serializer.JSON}

        while not self.canceled:
            connections = [self.sock] + list(self.sockets.keys())
            r, w, x = select.select(connections, [], [])
            for conn in r:
                if conn == self.sock:
                    client, address = self.sock.accept()
                    client.setblocking(False)
                    self.sockets[client] = Serializer.JSON
                else:
                    self.handle_request(conn, None)

            for conn in x:
                if conn in self.sockets:
                    self.sockets.pop(conn)

        self.sock.close()
                
