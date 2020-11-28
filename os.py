import os
from os import listdir
from os.path import isfile, join
print(os.getcwd())
mypath=os.getcwd()
onlyfiles = [f for f in listdir(mypath+"\\sentiments") if f!="_SUCCESS" and f!="_temporary"]
print(onlyfiles)
for f in onlyfiles:
    print(f)