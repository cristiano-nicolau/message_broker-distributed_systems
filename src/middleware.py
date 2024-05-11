"""Middleware to communicate with PubSub Message Broker."""
from collections.abc import Callable
from enum import Enum
from queue import LifoQueue, Empty
from typing import Any
import socket
import json, pickle, selectors
import xml.etree.ElementTree as ET

class MiddlewareType(Enum):
    """Middleware Type."""

    CONSUMER = 1
    PRODUCER = 2


class Queue:
    """Representation of Queue interface for both Consumers and Producers."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        """Create Queue."""
        self.host = "localhost"
        self.port = 5000
        self.topic = topic
        self._type = _type
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.host, self.port))
        
    def encode(self, type, topic , message ):
        """Encode data."""
        pass    

    def decode(self, data):
        """Decode data."""
        pass

    def push(self, value):
        """sends data to broker."""
        data = self.encode("publish", topic=str(self.topic), msg=value)
        self.sock.send(len(data).to_bytes(2, 'big') + data)

    def pull(self) -> (str, Any):
        """Receives (topic, data) from broker.

        Should BLOCK the consumer!"""

        data = self.sock.recv(2)
        header = int.from_bytes(data, 'big')
        data = self.sock.recv(header)
        
        if data != b"":
            data = self.decode(data)
            print(data)
            return data["topic"], data["msg"]

    def list_topics(self, callback: Callable):
        """Lists all topics available in the broker."""

        data = self.encode("list_topics")
        self.sock.send(len(data).to_bytes(2, 'big') + data)

    def cancel(self):
        """Cancel subscription."""
        data = self.encode("unsubscribe", topic=str(self.topic))
        self.sock.send(len(data).to_bytes(2, 'big') + data)



class JSONQueue(Queue):
    """Queue implementation with JSON based serialization."""
    
    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type)  
        msg ='{"type" : "serialization", "msg" : "json"}'
        self.sock.send(len(msg).to_bytes(2, 'big') + bytes(msg,"utf-8"))

        if self._type == MiddlewareType.CONSUMER:
            msg = self.encode("subscribe", topic=str(self.topic))
            self.sock.send(len(msg).to_bytes(2, 'big') + msg)

        
    def encode(self, type, topic = None, msg=None):
        if topic is not None and msg is not None:
            data = {"type": type, "topic": topic, "msg": msg}
        elif topic is not None:
            data = {"type": type, "topic": topic}
        elif msg is not None:
            data = {"type": type, "msg": msg}
        else:
            data = {"type": type}
        return (json.dumps(data).encode('utf-8'))
    
    def decode(self, data):
        return json.loads(data.decode('utf-8'))


class XMLQueue(Queue):
    """Queue implementation with XML based serialization."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type)

        msg = '{"type" : "serialization", "msg" : "xml"}'
        self.sock.send(len(msg).to_bytes(2, 'big') + bytes(msg,"utf-8"))

        if self._type == MiddlewareType.CONSUMER:
            msg = self.encode("subscribe", topic=str(self.topic))
            self.sock.send(len(msg).to_bytes(2, 'big') + msg)

    def encode(self,type, topic = None, msg=None):
        if (topic and msg) is not None:
            data = "<data><type>"+str(type)+"</type><topic>"+str(topic)+"</topic><msg>"+str(msg)+"</msg></data>"
        elif topic is not None:
            data = "<data><type>"+str(type)+"</type><topic>"+str(topic)+"</topic></data>"
        elif msg is not None:
            data = "<data><type>"+str(type)+"</type><msg>"+str(msg)+"</msg></data>"
        else:
            data = "<data><type>"+str(type)+"</type></data>"
        return data.encode('utf-8')
    
    def decode(self, data):
            tree = ET.ElementTree(ET.fromstring(data.decode("utf-8")))    
            data = {}
            
            for el in tree.iter():
                data[el.tag] = el.text

            return data
class PickleQueue(Queue):
    """Queue implementation with Pickle based serialization."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type)

        msg = '{"type" : "serialization", "msg" : "pickle"}'
        self.sock.send(len(msg).to_bytes(2, 'big') + bytes(msg,"utf-8"))

        if self._type == MiddlewareType.CONSUMER:
            msg = self.encode("subscribe", topic=str(self.topic))
            self.sock.send(len(msg).to_bytes(2, 'big') + msg)

    def encode(self, type, topic = None, msg=None):
        if topic is not None and msg is not None:
            data = {"type": type, "topic": topic, "msg": msg}
        elif topic is not None:
            data = {"type": type, "topic": topic}
        elif msg is not None:
            data = {"type": type, "msg": msg}
        else:
            data = {"type": type}
        return pickle.dumps(data)
    
    def decode(self, data):
        return pickle.loads(data)