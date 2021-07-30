import sys
import argparse
import middleware

class Consumer:
    def __init__(self, datatype):
        self.type = datatype
        self.queue = middleware.JSONQueue(f"/{self.type}")

    @classmethod
    def datatypes(self):
        return ["temp", "msg", "weather"]    

    def run(self, length=10):
        self.queue.listTopics(True)
        self.queue.listTopics()
        self.queue.unsub("/")
        self.queue.listTopics(True)
        while True:
            topic, data = self.queue.pull()
            print(topic, data)
            

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--type", help="type of producer: [temp, msg, weather]", default="temp")
    args = parser.parse_args()

    if args.type not in Consumer.datatypes():
        print("Error: not a valid producer type")
        sys.exit(1)

    p = Consumer(args.type)

    p.run()