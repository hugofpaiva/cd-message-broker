


# Computação Distribuída - Trabalho 1

## Message Broker

O objectivo deste trabalho é o desenvolvimento de um *Message Broker* capaz de interligar produtores (*Producers*) e consumidores (*Consumers*) através de um protocolo **PubSub** comum e de três mecanismos de serialização distintos (*XML*, *JSON* e *Pickle*).
Para além do *Message Broker* é necessário o desenvolvimento de um *middleware* que abstraia os produtores e consumidores de todo o processo de comunicação.

##  Preparação
Estas instruções ajudarão a executar os programas desenvolvidos.

### Requisitos
Para executar os programas é necessário ter instalado o *Python 3*. 
As próximas instruções devem ser executadas na raiz do repositório.

### Executar
Deve ser primeiramente executado o script *Python broker.py*, seguido de *consumer.py* ou *producer.py*, dependendo do objetivo do utilizador. 
Para executar as funcionalidades adicionais, os scripts *consumer.py* e/ou *producer.py* devem ser alterados de acordo com o objetivo.
Como forma de exemplo:

    python3 broker.py
    python3 consumer.py 
    python3 producer.py --type msg

## Detalhes

É possível encontrar todos os detalhes no [Relatório do Trabalho](/relatorio/CD_Message_Broker.pdf).

## Autores

 - **[Hugo Paiva de Almeida](https://github.com/hugofpaiva) - 93195** 
 - **[Carolina Araújo](https://github.com/carolinaaraujo00) - 93248**

 ## Nota
Classificação obtida de **19.2** valores em 20.
 
