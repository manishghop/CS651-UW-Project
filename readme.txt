install java
follow this website for pyspark:
https://naomi-fridman.medium.com/install-pyspark-to-run-on-jupyter-notebook-on-windows-4ec2009de21f
no need to pip install pyspark (don't do that)
install spark from apache spark website, I have downloaded 3.0.1 version

In this website https://github.com/steveloughran/winutils/blob/master/hadoop-2.7.1/bin/winutils.exe
go to hadoop 3.0.1 and download winutils.exe and save it to your spark installation file/bin folder.


Steps to execute files:
1) python trial4.py in one terminal and keep it running
then open browser and goto 127.0.0.1:3333 it will start displaying tweets in your terminal. Don't close the terminal.

2) in another terminal python trial3.py it will start pyspark task


