from enum import Enum
import socket
import json
import selectors
import sys
import fcntl
import os
import xml.etree.ElementTree as ET
import pickle

HOST = 'localhost'      # Address of the host running the server
PORT = 8000             # The same port as used by the server

orig_fl = fcntl.fcntl(sys.stdin, fcntl.F_GETFL)
fcntl.fcntl(sys.stdin, fcntl.F_SETFL, orig_fl | os.O_NONBLOCK)

class MiddlewareType(Enum):
    CONSUMER = 1
    PRODUCER = 2

class ProtocolType(Enum):
    JSON = 1
    XML = 2
    Pickle = 3

# A queue está associada a um CONSUMER ou a um PRODUCER, sendo que o último está associado a um tópico.
# Ambas as entidades utilizam um dado protocolo, sendo que inicialmente utilizam a função prot() para 
# darem a conhecer ao broker qual o protocolo usado.
class Queue:
    def __init__(self, topic, protocol, HOST, PORT, type):
        self.selector = selectors.DefaultSelector()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((HOST, PORT))
        self.selector.register(self.socket, selectors.EVENT_READ, self.pull)

        self.topic = topic
        self.protocol = protocol
        self.type = type

        self.prot() # Definir o protocolo no broker

        if type == MiddlewareType.CONSUMER:
            self.sub(topic) # Subscrever o tópico no broker

 
    # ENVIO DE MENSAGENS
    # O envio de mensagens é processado de igual modo para qualquer uma das operações a fazer: SUB, PUSH, LIST, UNSUB
    # As mensagens são construídas de acordo com o protocolo em questão, sendo depois calculado o tamanho das mesmas
    # em bytes, ficando esse valor como header. Ambos são encoded e então enviados. 
    def sendMsg(self, op, topic=None, value=None):
        if self.protocol == ProtocolType.JSON:
            msg = json.dumps({"op":op, "topic": topic, "value":value})

        elif self.protocol == ProtocolType.XML:
            msg = "<msg><op>" + str(op) + "</op><topic>" + str(topic) + "</topic><value>" + str(value) + "</value></msg>"

        elif self.protocol == ProtocolType.Pickle:
            msg = pickle.dumps({"op":op, "topic": topic, "value":value})

        if self.protocol == ProtocolType.Pickle:
            header = str(len(msg))
            hSize = len(header)
            header = 'z' * (4 - hSize) + header
            msg = header.encode('utf-8') + msg
            self.socket.sendall(msg)
        else:
            header = str(len(msg.encode('utf-8')))
            hSize = len(header)
            header = 'z' * (4 - hSize) + header

            msg = header + msg
            self.socket.sendall(msg.encode('utf-8'))


    # LISTAGEM DE TÓPICOS
    # Se a função for chamada sem qualquer tópico especificado, todos os tópicos guardados/iniciados serão listados
    # Caso contrário, apenas serão mostrados os tópicos associados ao consumer em questão. 
    def listTopics(self, topic=None):
        if topic: 
            self.sendMsg("list", str(True))
        else:
            self.sendMsg("list")


    # Enviar, por parte dos producers, novas publicações nos tópicos
    def push(self, value):
        self.sendMsg("push", self.topic, value)


    # Subscrever consumers a tópicos
    def sub(self, topic):
        self.sendMsg("sub", topic)

    
    # Pedido por parte de um consumer para deixar de subscrever a um tópico
    # No caso de não ser passado nenhum tópico, deixa de subscrever a todos os tópicos a que estava previamente subscrito
    def unsub(self, topic): 
        self.sendMsg("unsub", topic)


    # Definir o protocolo no broker - estabelecimento de uma primeira ligação com o broker e dar a conhecer o protocolo usado
    def prot(self):
        if self.protocol == ProtocolType.JSON:
            value = 'JSON'

        elif self.protocol == ProtocolType.XML:
            value = 'XML'

        elif self.protocol == ProtocolType.Pickle:
            value = 'Pickle'

        # Envio dos dados + header
        header = str(len(value))
        hSize = len(header)
        header = 'z' * (4 - hSize) + header
        msg = header + value

        self.socket.sendall(msg.encode('utf-8'))



    # Função utilizada pelo consumer para receber as mensagens publicadas nos tópicos a que está subscrito.
    # Primeiro faz-se uma recolha da informação do header: quantos bytes tem a mensagem que vai ser recebida.
    # Com base no protocolo usado pelo consumer, a data enviada é decoded, retornando apenas o tópico e o
    # value, que corresponde à mensagem em si. 
    def pull(self):
        header = self.socket.recv(4)
        data = header.decode('utf-8').replace('z', '')
        
        if data:
            data = self.socket.recv(int(data))

            if self.protocol == ProtocolType.JSON:
                decoded = json.loads(data.decode('utf-8'))
                return decoded.get("topic"), decoded.get("value")
                    
            elif self.protocol == ProtocolType.XML:
                decoded = data.decode('utf-8')

                XMLdata = ET.fromstring(decoded)

                topic = XMLdata.find("topic").text
                value = XMLdata.find("value").text

                return topic, value

            elif self.protocol == ProtocolType.Pickle:
                decoded = pickle.loads(data)
                return decoded.get("topic"), decoded.get("value")

        else:
            print('\x1b[0;37;41m' + 'Closing connection with broker' + '\x1b[0m')
            self.socket.close()
 

class JSONQueue(Queue):
    def __init__(self, topic, type=MiddlewareType.CONSUMER, protocol=ProtocolType.JSON):
        super().__init__(topic, protocol, HOST, PORT, type)

class XMLQueue(Queue):
    def __init__(self, topic, type=MiddlewareType.CONSUMER, protocol=ProtocolType.XML):
        super().__init__(topic, protocol, HOST, PORT, type)
        
class PickleQueue(Queue):
    def __init__(self, topic, type=MiddlewareType.CONSUMER, protocol=ProtocolType.Pickle):
        super().__init__(topic, protocol, HOST, PORT, type)