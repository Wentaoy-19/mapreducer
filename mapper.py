#Code based on https://blog.devgenius.io/big-data-processing-with-hadoop-and-spark-in-python-on-colab-bff24d85782f
import sys
import io
import re
import nltk
import pandas as pd  
import string

from nltk.stem import PorterStemmer
ps = PorterStemmer()

nltk.download('stopwords',quiet=True)
from nltk.corpus import stopwords
punctuations = '''!()-[]{};:'"\,<>./?@#$%^&*_~'''

stop_words = set(stopwords.words('english'))
input_stream = io.TextIOWrapper(sys.stdin.buffer, encoding='latin1')


def transform_text(text_str):
    test_str = text_str
    test_str = test_str.lower()
    test_str = test_str.strip()
    test_str = re.sub(' +', ' ', test_str)
    
    # Remove Punctuation
    # test_str = re.sub(r'[^\w\s]', ' ', test_str)
    test_str = test_str.translate(str.maketrans('', '', string.punctuation))
    test_str = test_str.translate(str.maketrans('', '', string.digits))
    doc = test_str.split(" ")
    
    # Remove stopwords
    filtered_words = [token for token in doc if token not in stopwords.words('english')]

    clean_text = ' '.join(filtered_words)
    # Change str into lower form
    test_str = clean_text.lower()
    test_str = test_str.strip()
    # remove all extra blank space
    test_str = re.sub(' +', ' ', test_str)
    return test_str



docid = 1 #actually line id
for line in input_stream:
  line = line.split(',', 3)[2]
  line = line.strip()
  line = re.sub(r'[^\w\s]', '',line)
  line = line.lower()
  for x in line:
    if x in punctuations:
      line=line.replace(x, " ")
  # line = transform_text(line)
      
  worddict = {}
  words=line.split()
  for word in words:
    if word not in stop_words and not word.isnumeric():
      if(bool(re.search(r'\d',word))):
        continue
      stemword = ps.stem(word)
      if(stemword in worddict):
        worddict[stemword] += 1
      else:
        worddict[stemword] = 1

  for w in worddict:
    print('%s %s %s' %(w,worddict[w],docid))  
  docid +=1 
