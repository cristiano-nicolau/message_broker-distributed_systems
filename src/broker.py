"""Message Broker"""
import enum
import socket
from typing import Dict, List, Any, Tuple
import socket, selectors, sys, json, pickle
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
        self._host = "localhost"
        self._port = 5000
        self.sel = selectors.DefaultSelector()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((self._host, self._port))
        self.sock.listen(10)
        self.sel.register(self.sock, selectors.EVENT_READ, self.accept)

        self.connections = {} # {socket : serialization}
        self.topics = {} # {topic : [values]}
        self.sub = {} # {(socket, serialization) : [topics]}

    def accept(self, sock, mask):
        """Accept new connection."""
        conn, addr = sock.accept()
        self.sel.register(conn, selectors.EVENT_READ, self.read)

    def read(self, conn, mask):
        """Read data from connection."""
        header = conn.recv(2)
        aux = int.from_bytes(header, "big")
        body = conn.recv(aux)

        if body != b"":
            if conn not in self.connections:
                serialization = "json"
            else:
                serialization = self.connections[conn]

            data = self.decode(body, serialization)
            print(data)

        else:
            data = None

        if data is not None:
            if conn not in self.connections:
                print(data)
                self.connections[conn] = data["msg"]

            if data["type"] == "publish":
                self.put_topic(data["topic"], data["msg"])
                for connection, topics in self.sub.items():
                    for tp in topics:
                        if data["topic"].startswith(tp):
                            try:
                                response = self.encode("published", data["topic"], data["msg"], self.connections[connection[0]])
                                connection[0].send(len(response).to_bytes(2, byteorder='big') + response)
                            except:
                                pass

            elif data["type"] == "subscribe":
                if data["topic"] is not None:
                    self.subscribe(data["topic"], conn)
                    last_value = self.get_topic(data["topic"])
                    if last_value != None:
                        response = self.encode("subscribed", data["topic"], last_value, self.connections[conn])
                        conn.send(len(response).to_bytes(2, byteorder='big') + response)

            elif data["type"] == "unsubscribe":
                self.unsubscribe(data["topic"], conn)
                response = self.encode("unsubscribed", data["topic"], None, self.connections[conn])
                conn.send(len(response).to_bytes(2, byteorder='big') + response)

            elif data["type"] == "list_topics":
                topics = str(self.list_topics())
                response = self.encode("topics", None, topics, self.connections[conn])
                conn.send(len(response).to_bytes(2, byteorder='big') + response)

        else:
            self.sel.unregister(conn)
            conn.close()
            print("Connection closed")
                
    def list_topics(self) -> List[str]:
        """Returns a list of strings containing all topics containing values."""
        return [key for key in self.topics]

    def get_topic(self, topic):
        """Returns the currently stored value in topic."""
        for tp in self.topics:
            if topic in tp:
                if len(self.topics[tp]) > 0:
                    return self.topics[tp][-1]
        return None
            
    def put_topic(self, topic, value):
        """Store in topic the value."""
        if topic in self.topics:
            self.topics[topic].append(value)
        else:
            self.topics[topic] = [value]

    def list_subscriptions(self, topic: str) -> List[Tuple[socket.socket, Serializer]]:
        """Provide list of subscribers to a given topic."""
        return [key for key, topics in self.sub.items() if topic in topics]

    def subscribe(self, topic: str, address: socket.socket, _format: Serializer = None):
        """Subscribe to topic by client in address."""
        if (address, _format) not in self.sub:
            self.sub[(address, _format)] = [topic]
        elif topic not in self.sub[(address, _format)]:
            self.sub[(address, _format)].append(topic)

    def unsubscribe(self, topic, address):
        """Unsubscribe to topic by client in address."""
        for sub in self.sub:
            if address in sub and topic in self.sub[sub]:
                self.sub[sub].remove(topic)

            
    def encode(self, type, topic, message, serialization):
        """Encode data using serialization."""
        if serialization == "xml":
            if topic is None:
                data = "<data><type>"+str(type)+"</type><msg>"+str(message)+"</msg></data>"
            else:
                data = "<data><type>"+str(type)+"</type><topic>"+str(topic)+"</topic><msg>"+str(message)+"</msg></data>"
            return data.encode("utf-8")
        else:
            if topic is None:
                data = {"type" : type, "msg" : message}
            else:
                data = {"type" : type, "topic" : topic, "msg" : message}
            
            if serialization == "json":
                return json.dumps(data).encode('utf-8')
            elif serialization == "pickle":
                return pickle.dumps(data)
        
    def decode(self, data, serialization):
        """Decode data using serialization."""
        if serialization == "json":
            return json.loads(data.decode('utf-8'))
        elif serialization == "pickle":
            return pickle.loads(data)
        elif serialization == "xml":
            tree = ET.ElementTree(ET.fromstring(data.decode("utf-8")))    
            data = {}
            
            for el in tree.iter():
                data[el.tag] = el.text

            return data
    

    def run(self):
        """Run until canceled."""

        while not self.canceled:
            try:
                events = self.sel.select()
                for key, mask in events:
                    callback = key.data
                    callback(key.fileobj, mask)
            except KeyboardInterrupt:
                print("Closing server, please wait...")
                self.canceled = True
                sys.exit()
            except socket.error:
                print("Error in sockets, closing server")
                sys.exit()