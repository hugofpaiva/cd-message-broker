from enum import Enum

class MiddlewareType(Enum):
    CONSUMER = 1
    PRODUCER = 2

class Tree:
    def __init__(self, topic, parent, path, consumers = None, childrens=None):
        self.path = path
        self.topic = topic
        self.last_msg = None
        self.consumers = []
        self.children = []
        self.parent = parent
        
        if childrens is not None:
            for child in childrens:
                self.children.append(child)
        
        if consumers is not None:
            for cons in consumers:
                self.consumers.append(cons)
        

    # Retornar a(s) última(s) mensagen(s) associada(s) a um tópico.
    # Se esse tópico tem nós descendentes, o objetivo é que o consumer receba as últimas mensagens de todos eles.
    def getLastMsg(self):
        # Retorna uma lista de strings do tipo "topic-message"
        msg =[]
        if self.last_msg is not None:
            msg.append(f"{self.path}-{self.last_msg}")

        if self.children:   
            for c in self.children:
                msgTemp = c.getLastMsg()

                for msgT in msgTemp:
                    msg.append(msgT)
        return msg


    # Retornar o caminho completo de um nó (o próprio nó precedido pelos parents)
    def getPath(self):
        return self.path

    # Retorna a lista de todos os consumers subscritos a este nó
    def getConsumers(self):
        return self.consumers

    # Retorna o tópico deste nó
    def getTopic(self):
        return self.topic

    # Retorna a lista de filhos deste nó
    def getChildrens(self):
        return self.children

    # Retorna todos os paths existentes que começam no nó em questão
    # ["weather/humidity", "weather/pressure", "weather/temp/wind]
    def getTopics(self):

        # Caso o nó não tenha filhos, o path é o nó em si
        if len(self.children) == 0:
            return [self.path]

        # Adicionar-se-á ao array a devolver cada path existente
        else:
            topics=[]
            topics.append(self.path)

            for child in self.children:
                topicsChildren = child.getTopics()

                for topic in topicsChildren:
                    topics.append(topic)
        return topics


    # Retorna o nó-pai de que provém
    def getParent(self):
        return self.parent

    # Retornar os consumers aos quais é suposto enviar a mensagem do nó em questão. 
    # Enviar a todos os consumers desse nó bem como aos consumers que estão subscritos aos nós-parent (até à raiz do path a que o nó pertence)
    def getAllRecipients(self):
        recipients=[]

        for recipient in self.consumers:
            recipients.append(recipient)

        parent = self.getParent()
        while parent is not None:
            for recipient in parent.getConsumers():
                recipients.append(recipient)

            parent = parent.getParent()

        return recipients


    # Retorna o nó-filho cujo tópico é igual àquele que foi passado como argumento 
    # Se nenhum dos filhos tiver esse tópico, retorna None
    def getChildren(self, children_topic):
        for child in self.children:
            if child.getTopic() == children_topic:
                return child
        return None

    # Atualizar a última mensagem associada a um tópico - usada quando são feitas publicações por parte dos producers
    def setLastMsg(self, value):
        self.last_msg = value

    
    # Adicionar filhos ao longo da árvore deste nó consoante o array children passado, que contém os tópicos dos nós-filhos.
    # Se já existir o consumer num dos filhos-superiores passados, é retornada uma flag(permissionToAdd) como False para que não seja possível
    # ser adicionado ao nó-filho mais abaixo. Esta função retorna também o último nó correspondente ao último tópico da lista children passada.
    def addChildren(self, children, conn=None):
        permissionToAdd = True  

        # child contém agora o filho DIRETO que tenha o tópico em questao (ou então é None)
        child = self.getChildren(children[0])

        # Primeiro será verificado se a conn já está registada num nó (seja ele A) que esteja acima do nó em questão,
        # uma vez que, neste caso, a conn já recebe as mensagens produzidas em nós-filhos de A. 
        if conn in self.consumers:
            permissionToAdd = False

        # Caso o child contenha o filho direto,       
        if child is not None:
            # Adicionam-se os restantes filhos
            if len(children)>1:
                return child.addChildren(children[1:len(children)], conn)
            # Caso contrário, apenas se retorna o filho
            else:
                return child, permissionToAdd

        # Caso não exista o filho direto, será adicionado o novo filho ao ramo deste nó (e os seus respetivos filhos)
        else: 
            child = Tree(children[0], self, str(f"{self.path}/{children[0]}"))
            if len(children)>1:
                self.children.append(child)
                return child.addChildren(children[1:len(children)], conn)
            else:
                self.children.append(child)
                return child, permissionToAdd



    # Adicionar um consumer a um nó
    # Consideram-se duas situações: pretende-se adicionar um consumer a um nó que é raiz de um path - "weather"
    #                               pretende-se adicionar um consumer a um nó descendente da raiz de um path - "weather/temp/wind"
    # Na primeira situação childs = None, pelo que o consumer é logo adicionado ao nó em questão, após verificação se existe nos nós descendentes.
    # Se existir, será removido dos nós em baixo e adicionado ao nó em questão, continuando assim a receber as mensagens dos tópicos inferiores.
    # Na segunda situação childs = ['temp', 'wind'], o pressuposto é que se pretende adicionar o consumer ao último nó do path.
    # Qualquer nó do path que ainda não exista será criado. Nos nós já existentes é sempre feita uma verificação da pertença ou não do consumer 

    # Adicionar um consumer
    def addConsumer(self, consumer, children=None):
        node = None
        
        # Se não for passado nenhum array, adiciona-se o consumer diretamente neste nó
        if children is None:
            if consumer not in self.consumers:
                # Inicialmente procura-se o consumer nos nós abaixo
                nodes = self.searchConsumer(consumer)
                
                if self in nodes: 
                    nodes.remove(self) 

                # Se este consumidor estiver nos nós em baixo será removido de todos esses nós, visto que vai ser adicionado a um nó superior
                for x in nodes: 
                    x.rmvConsumer(consumer)
            
                self.consumers.append(consumer)
            else:
                print('\x1b[0;37;41m' + 'Consumer already on the node!' + '\x1b[0m')

            node = self
        
        # No caso de ser passado array, vai verificar se esse consumer já existe nos nós-filhos, adicionando-o aos mesmos no caso de ainda não existirem.
        # Se os nós-filhos ainda não tiverem sido instanciados, são criados e então é-lhes adicionado o consumer. Esta função retorna, além do nó onde
        # tem de ser adicionado o consumer, uma permissão se este pode ser adicionado. Isto acontece porque se o consumer já existir num dos filhos 
        # superiores ao filho que se quer adicionar, não faz sentido fazer essa adição.
        else: 
            node, permission = self.addChildren(children, consumer)

            if permission:
                # Inicialmente procura-se o consumer nos nós abaixo
                nodes = node.searchConsumer(consumer) 
                if self in nodes:
                    nodes.remove(self)

                # Se este consumidor estiver nos nós em baixo será removido de todos esses nós, visto que vai ser adicionado a um nó superior
                for x in nodes: 
                    x.rmvConsumer(consumer)
                node.addConsumer(consumer)
            else:
                print('\x1b[0;37;41m' + 'Consumer already on superior node!' + '\x1b[0m')

        return node


    # Remover um consumer
    # Se não tiver sido passado nenhum array, primeiro verificar se o nó recebido tem o consumer
    # Se tiver, remove-se. Se não tiver e tiver filhos, tem de se ir a todos remover o consumer 
    def rmvConsumer(self, consumer, children=None):
        found = False
        if children is None:
            if consumer in self.consumers:
                self.consumers.remove(consumer)
                return self
            
            # Ir aos filhos e remover em todos eles
            else:
                for child in self.children:
                    x = child.rmvConsumer(consumer)
                    if x is not None:
                        found = True
                if found:
                    return x
                else:
                    return None
            

        # Caso o array passado não seja vazio, vamos percorrendo o ramo, para baixo, nó a nó (com base nos tópicos passados).
        # Se um dos nós encontrados contiver o consumer, este é removido, porque se tem a certeza de que não está mais abaixo.
        else: 
            for child in self.children:
                if child.getTopic() == children[0]: 
                    if consumer in child.getConsumers():
                        child.consumers.remove(consumer)
                        print('\x1b[5;30;42m' + 'Consumer removed on the node with the topic: '+ child.getPath() + '\x1b[0m')
                        return child
                    else:
                        if len(children) > 1: 
                            return child.rmvConsumer(consumer, children[1:len(children)])
                        else:
                            for kid in child.children:
                                x = kid.rmvConsumer(consumer)
                                if x is not None:
                                    found = True
                            if found:
                                return x
                        
        return None

    
    # Vai procurar nos filhos, usando o array de forma similar à função addConsumer 
    def searchChildren(self, children):
        for child in self.children:

            if(child.getTopic() == children[0]):
                if len(children)>1:
                    node = child.searchChildren(children[1:len(children)])
                    return node
                else:
                    return child          
        return None


    # Retorna os nós, ou nó, cujos tópicos são subscritos pelo consumer em questão
    def searchConsumer(self, conn):
        if conn in self.consumers:
            return [self]
        else:
            nodes = []

            if len(self.children) > 0:
                for child in self.children:
                    nodes.extend(child.searchConsumer(conn))
            return nodes
    

  
      