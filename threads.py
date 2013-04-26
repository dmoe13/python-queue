#!/usr/bin/python

import sys
import threading
import Queue
import re
from pymongo import MongoClient

readSize = 1024

class WorkItem(object):
    def __init__(self, text):
        self.text = self.sanitizeText(text)

    @staticmethod
    def sanitizeText(text):
        # punctuation - these SHOULD be replaced with a regex, but this will work for now
        text = text.replace('$', ' ')        
        text = text.replace(',', ' ')
        text = text.replace('.', ' ')
        text = text.replace('!', ' ')
        text = text.replace('?', ' ')
        text = text.replace(':', ' ')
        text = text.replace(';', ' ')
        text = text.replace("'", ' ')
        text = text.replace('"', ' ')
        text = text.replace('(', ' ')
        text = text.replace(')', ' ')
        text = text.replace('-', ' ')
        text = text.replace('\\', ' ')
        text = text.replace('/', ' ')

        # iffy white space        
        text = text.replace('\n', ' ')
        text = text.replace('\r', ' ')

        # get rid of consecutive spaces since that's what we're splitting on
        text = re.sub('\s+', ' ', text)

        return text

    def getWordCount(self):
        return len(self.text.split(" "))

    def getCountPerWord(self):
        words = {}
        for word in self.text.split(" "):
            word = word.lower()
            if word in words:
                words[word] = words[word] + 1
            else:
                words[word] = 1
        return words

class Aggregator(threading.Thread):
    def __init__(self, queue, client):
        threading.Thread.__init__(self)
        self.queue = queue
        self.results = client.test.results

    def run(self):
        while True:
            item = self.queue.get()
            self.results.insert(item.getCountPerWord())
            self.queue.task_done()

def main():
    if (len(sys.argv) != 3):
        print "usage: thread.py <inputfile> <num_threads>"
        return

    filePath = sys.argv[1]

    try:
        numWorkers = int(sys.argv[2])
    except ValueError:
        print "Invalid value for num_threads."
        return

    queue = Queue.Queue()
    client = MongoClient()

    # clear out existing results for this pass through
    client.test.results.remove()

    #spawn a pool of workers
    for i in range(numWorkers):
        p = Aggregator(queue, client)
        p.setDaemon(True)
        p.start()

    try:
        with open(filePath, 'r') as file:
            # loop though line by line, appending onto text
            # once over the theshold add the text to the queue
            text = ""
            for line in file:
                text += line
                if len(text) > readSize:
                    queue.put(WorkItem(text))
                    text = ""

            # put any left over text on the queue
            if len(text) > 0:        
                queue.put(WorkItem(text))
    except IOError:
        print "Error opening file."
        return

    # wait for all of the threads to finish working
    queue.join()

    # pull all the records that got inserted into mongo and combine them
    finalCounts = {}
    for threadResults in client.test.results.find():
        for key in threadResults:
            if key == '_id': continue
            if key in finalCounts:
                finalCounts[key] = finalCounts[key] + threadResults[key]
            else:
                finalCounts[key] = threadResults[key]

    for word, count in finalCounts.iteritems():
        print word + ": " + str(count)

    print "Total number of distinct words: " + str(len(finalCounts))

if __name__ == "__main__":
    main()
