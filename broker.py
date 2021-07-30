import selectors
import socket
import json
from tree import Tree
from enum import Enum
import xml.etree.ElementTree as ET
import pickle
from collections import deque

sel = selectors.DefaultSelector()


connProtocol = {}      # Dicionário cujas keys são as conexões e o value correspondente é protocolo usado por essas mesmas conexões {conn:protocol}
rootTopics = []        # Lista com a root (1º tópico) de cada cadeia de tópicos hierárquicos
queue = deque()        # Deque com os vários clientes à espera de ser atendidos com os dados guardados da seguite forma: [[conn, data]]


def accept(sock, mask):
    conn, addr = sock.accept()
    print('accepted', conn, 'from', addr, "\n")
    conn.setblocking(False)
    sel.register(conn, selectors.EVENT_READ, read)


# ENVIO DE MENSAGENS
# O envio de mensagens é processado de igual modo para qualquer uma das operações a fazer: SUB, PUSH, LIST, UNSUB
# As mensagens são construídas de acordo com o protocolo em questão, sendo depois calculado o tamanho das mesmas
# em bytes, ficando esse valor como header. Ambos são encoded e então enviados. 
def sendMsg(conn, topic, value):
    if connProtocol.get(conn) == "JSON":
        msg = json.dumps({"op":"msg", "topic":topic, "value":value})

    elif connProtocol.get(conn) == "XML":
        msg = "<msg><op>" + "msg" + "</op><topic>" + str(topic) + "</topic><value>" + str(value) + "</value></msg>"

    elif connProtocol.get(conn) == "Pickle":
        msg = pickle.dumps({"op":"msg", "topic":topic, "value":value})


    if connProtocol.get(conn) == "Pickle":
        header = str(len(msg))
        hSize = len(header)
        header = 'z' * (4 - hSize) + header

        msg = header.encode('utf-8') + msg
        conn.sendall(msg)
    else:
        header = str(len(msg.encode('utf-8')))
        hSize = len(header)
        header = 'z' * (4 - hSize) + header
    
        msg = header + msg
        conn.sendall(msg.encode('utf-8'))


def read(conn, mask):
    # Receber apenas os primeiros 4 bytes para ler o header e assim saber quantos bytes tem o resto da mensagem
    data = conn.recv(4)
    data = data.decode('utf-8').replace('z', '')

    if data:
        # Receber o resto da mensagem
        data = conn.recv(int(data))
        # Adicionar a conexão e a mensagem recebida à queue para poder ser eventualmente processada
        queue.append([conn, data])

     # Termínio de uma conexão.
    else:
        close(conn)

def close(conn):
    # Remover da queue se ainda tiver alguma tarefa pendente, visto que a conexão fechou
    for listConn in list(queue):  
        if conn == listConn[0]:
            queue.remove(listConn)

    # Remover a conn do dicionário de serialização, visto que a conexão fechou
    if conn in connProtocol.keys(): 
        del connProtocol[conn]

    # Remover o consumer de todos os tópicos subscritos, visto que a conexão fechou. No caso de ser producer, não vai fazer nada
    for key in rootTopics: 
        key.rmvConsumer(conn)

    print('closing', conn, "\n")
    sel.unregister(conn)
    conn.close()


def main(conn, data):
    # Ao receber data, o broker verifica se se trata de uma das seguintes situações:
    # 1. Já conhece a conexão. A data poderá, então, ser um pedido de, no caso do CONSUMER: a) SUB   c) LIST   d) UNSUB
    #                                                                  no caso do PRODUCER: b) PUSH   
    if conn in connProtocol.keys():
        # Protocolo associado a esta conexão   
        protocol = connProtocol.get(conn)   

        # A data recebida vai ser decoded de acordo com o protocolo registado
        if protocol == "JSON":
            decoded = json.loads(data.decode('utf-8'))
            op = decoded.get("op")
            topic = decoded.get("topic")

        elif protocol == "XML":
            decoded = data.decode('utf-8')
            XMLdata = ET.fromstring(decoded)
            op = XMLdata.find("op").text
            topic = XMLdata.find("topic").text
            if topic == "None":
                topic = None

        elif protocol == "Pickle":
            decoded = pickle.loads(data)
            op = decoded.get("op")
            topic = decoded.get("topic")
        
        else:
            print('\x1b[0;37;41m' + 'Protocol unknown!' + '\x1b[0m')
            

        #    1.a) SUB: Adicionar a conn do consumer ao nó da árvore que corresponde ao tópico a que se pretende subscrever,
        #              o que fará com que ele receba a última mensagem desse tópico e dos seus filhos, em conjunto com
        #              qualquer mensagem futura (até, e se, cancelar a subscrição). Caso o tópico ainda não exista, será
        #              criado um novo nó para o mesmo. Caso o consumidor já esteja subscrito a um tópico superior, visto 
        #              que este já recebe as mensagens de todos os tópicos descendentes, não será adicionado como consumidor
        #              desse nó mais abaixo. Há ainda a possibilidade de o consumer se subscrever em todos os tópicos já
        #              registados no broker, até ao momento, passando "/" como tópico.
        if op == "sub":
            # Para subscrever a todos os tópicos registados no broker
            if topic == "/":
                for key in rootTopics:
                    node = key.addConsumer(conn)
                    print("Consumer subscribed to topic "+node.getPath()+"\n")
                    last_msg = node.getLastMsg()

                    # Se houver, serão enviadas ao consumer a última mensagem deste tópico, enviando também as mensagens dos tópicos-filho
                    if last_msg:
                        print("Sending last messages to the consumer who just subscribed to the "+node.getPath()+"\n")
                        sendMsg(conn, "","\nPrinting the last messages from " + str(topic))

                        for msg in last_msg:
                            topic_msg = msg.split("-") 
                            sendMsg(conn, topic_msg[0], topic_msg[1])

                    else:
                        print("No messages have been yet published in " + str(topic) +"\n")

            # Para subscrever a um tópico em específico
            else: 
                if topic[0] != "/":
                    print('\x1b[0;37;41m' + 'Invalid Topic!' + '\x1b[0m')
                    return
                topics = topic.split("/")
                topics.remove('')
            
                # É preciso fazer uma verificação - caso o nó esteja inscrito em /weather, não pode inscrever-se em /weather/temp
                found = False              
                for key in rootTopics:
                    
                    if topics[0] == key.getTopic():                       
                        del topics[0]

                        if len(topics) >= 1:
                            node = key.addConsumer(conn, topics)
                        else:
                            key.addConsumer(conn)
                            node = key

                        found = True
                        break
                    
                # Caso o nó ainda não exista, será criado 
                if not found:
                    node = Tree(topics[0], None, str("/"+topics[0]))  
                    rootTopics.append(node) 
                    del topics[0]

                    if len(topics) >= 1:
                        node = node.addConsumer(conn, topics)
                    else:
                        node = node.addConsumer(conn)  
                
                print("Consumer subscribed to topic "+node.getPath()+"\n")


                # Se houver, serão enviadas ao consumer a última mensagem deste tópico, enviando também as mensagens dos tópicos-filho
                last_msg = node.getLastMsg()

                if last_msg:
                    print("Sending last messages to the consumer who just subscribed to the "+node.getPath()+"\n")
                    sendMsg(conn, "","\nPrinting the last messages from " + str(topic))

                    for msg in last_msg:
                        topic_msg = msg.split("-") 
                        sendMsg(conn, topic_msg[0], topic_msg[1])

                else:
                    print("No messages have been yet published in " + str(topic) +"\n")



        #    1.b) PUSH: As mensagens de PUSH têm a elas associado um dado value que corresponde à mensagem a publicar pelo producer.
        #               Iremos então percorrer todos os consumers subscritos ao tópico em que o producer pretende publicar
        #               e ser-lhes-á enviada a nova publicação, no protocolo correspondente.    
        #               Há a possibilidade de o producer publicar uma mensagem em todos os tópicos registados no broker, caso seja 
        #               passado "/" como topic.
        elif op == "push":
            if protocol == "JSON" or protocol == "Pickle":
                value = decoded.get("value")
            elif protocol == "XML":
                value = XMLdata.find("value").text
            
            # Verificar se o producer está registado no tópico em que pretende publicar, só se estiver é que publica

            
            if topic[0] != "/":
                print('\x1b[0;37;41m' + 'Invalid Topic!' + '\x1b[0m')
                return
            topics = topic.split("/")
            topics.remove('')
            found = False 
            for key in rootTopics:
                
                if topics[0] == key.getTopic():
                    del topics[0] 
                
                    if len(topics) >= 1:
                            node, permission = key.addChildren(topics)
                    else:
                        node = key

                    found = True
                    break

                    
            if not found:
                node = Tree(topics[0], None, str("/"+topics[0]))  
                rootTopics.append(node) 
                del topics[0]

                if len(topics) >= 1:
                    node, permission = node.addChildren(topics)  
                print('\x1b[5;30;42m' + 'New node created!' + '\x1b[0m')
    
            node.setLastMsg(value)
            recipients = node.getAllRecipients()

            for recipient in recipients:
                sendMsg(recipient, topic, value)

            print("Producer of the topic " + str(topic) + " just sent a message\n")
    


        #    1.c) LIST: A mensagem associada à função de listagem pode ser chamada de duas maneiras, dependendo do que é pretentido.
        #               Para listar TODOS os tópicos em vigor, isto é, todos os tópicos que já foram criados no broker, a mensagem de
        #               listagem é enviada com topic=None, value=None. Para que sejam listados APENAS os tópicos associados ao consumer
        #               em questão, então topic=True
        elif op == "list":
            nodesListing = []
            
            # Listagem APENAS dos tópicos subscritos pelo consumer
            if topic == "True":
                if protocol == "JSON" or protocol == "Pickle":
                    value = decoded.get("value")
                elif protocol == "XML":
                    value = XMLdata.find("value").text

                
                for topic in rootTopics:
                    nodesListing.extend(topic.searchConsumer(conn))

                topicsSub = []
                for node in nodesListing:
                    topicsSub.extend(node.getTopics())

                value = ''
                for topic in topicsSub:
                    value += str("\n"+topic)
                
                if value == '':
                    sendMsg(conn,"\nThis consumer is not subscribed to any topics...", value)
                else:
                    value += str("\n")
                    sendMsg(conn,"\nListing all topics this consumer is subscribed to...", value) 


            # Listagem de TODOS os tópicos registados
            else:
                value = ''
                topicsSub = []
                for topic in rootTopics:
                    topicsSub.extend(topic.getTopics())
                for path in topicsSub:
                    value += str("\n"+path)
                if value != '':
                    value += str("\n")
                    sendMsg(conn,"\nListing all registered topics...", value) 
                else:
                    sendMsg(conn,"\nThere are no registered topics", value)



        #    1.d) UNSUB: O consumer tanto pode pedir para ser unsubed de todos os tópicos a que estava subscrito, passando "/" ao chamar   
        #                a função, como pode ser simplesmente cancelar a subscrição de apenas um tópico em concreto, passando esse mesmo como argumento.
        elif op == "unsub":     
            # Remover o consumer do tópico a que estava subscrito. Se esse tópico originar outros tantos, não vale a pena ir aos nós-descendentes
            # remover o consumer porque apenas o tópico-pai guarda a conexão do mesmo.
            found = False 
            if topic != "/":
                if topic[0] != "/":
                    print('\x1b[0;37;41m' + 'Invalid Topic!' + '\x1b[0m')
                    return     
                topics = topic.split("/")
                topics.remove('')           
                for key in rootTopics:
                    if topics[0] == key.getTopic():                       
                        del topics[0]

                        if len(topics) >= 1:
                            node = key.rmvConsumer(conn, topics)
                        else:
                            node = key.rmvConsumer(conn)

                        found = True
                        
                        if node is not None:
                            sendMsg(conn, "","\n\nSuccessfully unsubscribed from " + str(topic))
                            print('\x1b[5;30;42m' + 'Consumer has been successfully unsubscribed from ' + str(topic) + ' and all their children.' + '\x1b[0m')
                        else:
                            sendMsg(conn, "","\nConsumer can\'t be unsubscribed from a topic (" + str(topic) + ") that doesn\'t exist or that he wasn\'t already subscribed to.\n")
                            print('\x1b[0;37;41m' + 'Consumer can\'t be unsubscribed from a topic (' + str(topic) + ') that doesn\'t exist or that he wasn\'t already subscribed to.\n' + '\x1b[0m')

                        break
    
                if not found:
                    sendMsg(conn, "","\n Consumer can\'t be unsubscribed from a topic (" + str(topic) + ") that doesn\'t exist or that he wasn\'t already subscribed to.\n")
                    print('\x1b[0;37;41m' + 'Consumer can\'t be unsubscribed from a topic (' + str(topic) + ') that doesn\'t exist or that he wasn\'t already subscribed to.' + '\x1b[0m')
            
            # No caso de o tópico passado ser "/", será feito unsub deste consumer a todos os tópicos a que estava subscrito previamente
            else: 
                for key in rootTopics:
                    node = key.rmvConsumer(conn)
                    if node is not None:
                        found = True
                
                if not found:
                    sendMsg(conn, "","\nConsumer is not subscribed in any topic.\n")
                    print('\x1b[0;37;41m' + 'Consumer is not subscribed in any topic.' + '\x1b[0m')
                else:
                    sendMsg(conn, "","\nConsumer has been successfully unsubscribed from all topics he was previously subscribed to.\n")
                    print('\x1b[5;30;42m' + 'Consumer has been successfully unsubscribed from all topics he was previously subscribed to.' + '\x1b[0m')
            
           

    # 2. Não conhecendo a conexão, será adicionada uma entrada no dicionário tal que conn:protocolo
    else:
        decoded = data.decode('utf-8')
        if decoded == "JSON" or decoded == "XML" or decoded == "Pickle":
            connProtocol[conn] = decoded
        else:
            print('\x1b[0;37;41m' + 'Protocol unknown!' + '\x1b[0m')
            close(conn)

    return

   



sock = socket.socket()
sock.bind(('', 8000))
sock.listen(100)                                       # Número de ligações em simultâneo suportadas
sock.setblocking(False)                                # Cria a socket e coloca como não bloqueante
sel.register(sock, selectors.EVENT_READ, accept)       # Registamos a socket e quando há um evento, esta socket vai correr a função accept


while True:
    # Fica bloqueado até um ou mais eventos acontecerem, retornando os eventos
    events = sel.select(0.1)

    # A key dá acesso à socket e à função que foi definida (no 3º parâmetro do register - accept)
    for key, mask in events:
        callback = key.data
        callback(key.fileobj, mask)
    
    
    try:
        nextInQueue = queue.popleft()  
        main(nextInQueue[0], nextInQueue[1]) 
    except IndexError:
        pass
