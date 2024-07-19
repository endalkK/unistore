
import threading
import sys
import time
from collections import deque

# to store the transaction read from the file by the producer
fifo = deque()

# max number of entries allowed in the fifo object
maxFifo = sys.argv[4]

# counter to set the internalId of each transaction
idCounter = 0

# --------------------------------------
# Add a lock for idCounter
idCounterLock = threading.Lock()  # for 1

# Add an event for file reading synchronization
fileEvent = threading.Event()  # for 2

# Add an event for consumer thread to stop at the end of the file
consumerEvent = threading.Event()  # for 4
# Add a lock for consumerEvent setting
consumerEventLock = threading.Lock()

# Add a condition for FIFO synchronization
fifoSemaphore = threading.Semaphore(int(maxFifo))  # for 3

# ------------------------------------

# Transaction object to store the transactionid, producerSleep, and consumerSleep
# the sleep time is in 1000s of a second
class Transaction:
    def __init__(self, transactionId, producerSleep, consumerSleep):
        self.transactionId = transactionId
        self.producerSleep = producerSleep
        self.consumerSleep = consumerSleep
        self.internalId = 0
start = time.time()
# function used by the producer to read the file, and to queue the transaction in the global fifo
def threadProducer(filename):
    global idCounter
    with open(filename, 'r') as file:
        #to make sure transactionFile is not being closed while the consumer threads are still trying to read from it. 
        for line in file:
            params = line.split(",")
            t = Transaction(params[0], params[1], params[2])

            #synchronize between the producers the idCounter
            with idCounterLock:
                idCounter += 1
                t.internalId = idCounter

            with fifoSemaphore:
                # wait until there is space in the FIFO
                #synchronize between the producers and the consumers, to have the producer stop when the FIFO is full(based on the maxFifoDepth)
                fifo.append(t)
                print('Producer:' + t.transactionId + ' internalId:' + str(t.internalId))
            time.sleep(float(t.producerSleep) / 1000)
        
        # Signal the end of the file
        #synchronize between the producers
        fileEvent.set()  
        with consumerEventLock:
            consumerEvent.set() #synchronize between the consumers and the producers, to have the consumer threads stop when the end of file is reached
        print('Producer completed')

# function used by the consumer to get the transaction from the global fifo
# it stops when the transactinoId is 9999
def threadConsumer():
    while True:
        with fifoSemaphore:
            #synchronize between the producers and the consumers, to have the consumers stops when the FIFO is empty
            if len(fifo) == 0 and not fileEvent.is_set() and not consumerEvent.is_set():
                # print("No transaction in the deque.")
                continue
            if len(fifo) == 0 and not fileEvent.is_set():
                # No more transactions, but the producer may produce more
                continue

            if len(fifo) == 0 and consumerEvent.is_set():
                print("Consumer Completed")
                break

            if len(fifo) > 0:
                t = fifo.popleft()
                if int(t.transactionId) == 9999:
                    print("Consumer Completed")
                    break
                print('Consumer:' + t.transactionId + ' internalId:' + str(t.internalId))
                time.sleep(float(t.consumerSleep) / 1000)

# it receives as input the name of the transactionFile, the number of producer, the number of consumer as parameters
# and the maximum number of transactions which are allowed at once in the fifo
threads = []

print('Starting producer:' + sys.argv[2])
for _ in range(int(sys.argv[2])):
    t = threading.Thread(target=threadProducer, args=(sys.argv[1],))
    t.start()
    threads.append(t)

print('Starting consumer:' + sys.argv[3])
for _ in range(int(sys.argv[3])):
    t = threading.Thread(target=threadConsumer, args=())
    t.start()
    threads.append(t)

for thread in threads:
    thread.join()
end = time.time()
#print("time takes: ", end-start)