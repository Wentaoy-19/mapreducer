# Code based on https://blog.devgenius.io/big-data-processing-with-hadoop-and-spark-in-python-on-colab-bff24d85782f
from operator import itemgetter
import sys

current_word = None
current_count = 0
word = None
countdict = {}
outstr = ""

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    line=line.lower()

    # parse the input we got from mapper.py
    word, count,docid = line.split(' ')
    
    try:
      count = int(count)
      docid = int(docid)
    except ValueError:
      continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_word == word:
        outstr += (" "+str(docid)+":"+str(count))
        current_count += count
    else:
        if current_word:
            print(outstr)
        outstr = word + (" " + str(docid) + ":" +str(count))
        current_count = count
        current_word = word

# do not forget to output the last word if needed!

if(current_word == word):
  if(current_word):
    print(outstr)

# if current_word == word:
#     print( '%s	%s' % (current_word, current_count))
