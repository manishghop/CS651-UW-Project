# CS651 Project

This project is for the CS651 final project.



We were given a task to apply the big data tools that were learnt in the class combined with some machine learning models to solve a real world problem.

We decided to predict the winner of US presidential elections 2020 out of the two contestants(Trump,Biden). 

As it is a binary classification problem, we used several binary classifiers such as Logistic Regression, Naive Bayes Model for prediction. We also applied sentimental analysis and figured out from the tweets who'll win the presidential elections.




# US Presidential Election 2020 Winner


## PACKAGES
<h1>You need to install following packages first</h1>
<ul>
<li>sklearn </li>
<li>pandas </li>
<li> numpy </li>
<li> findspark</li>
<li> re </li>
<li> subprocess </li>
<li> sys </li>
<li> pyspark </li> 
<li> tweepy</li>
<li> socket </li>
</ul>
<p>Go ahead and pip install above packages.</p>

```
pip install pyqt5
pip install sklearn
pip install pandas
pip install numpy
pip install dbConnect
pip install sklearn
pip install pandas
pip install numpy 
pip install findspark
pip install re 
pip install subprocess 
pip install sys 
pip install pyspark  
pip install tweepy
pip installsocket 
```


<h2>To run the file simply </h2>

Open two terminal if on MAC OS, if on Windows open Command Prompt or in UNIX open Command Prompt/Shell.

In one terminal Run 

```
python stream_producer.py
```

In the other terminal Run

```
python stream_consumer.py
```

## DATA-GATHERING

The data source was the tweets and they were collected in real-time from the twitter. As the data was huge we use a Big Data framework Spark Streaming to fetch the tweets in real time. We assigned a time window of 15 minutes for gathering the tweets.



## DATA PRE-PROCESSING

In this step we look for any discrepancies in the dataset. As the tweets can contain unnecassary things , it should be cleaned and processed before passing it to a model.





## FEATURE-ENGINEERING

As our problem dealt with text, we had only a single text feature, we didn't need to remove any other features. The feature engineering we applied was to 
remove emojis, unwanted text, explain here more.



## MODEL SELECTION

Our problem was a binary classification, so we used Logistic Regression & Naive Bayes Model. 

## MODEL EVALUATION & COMPARISION

Out of these two, Naive Bayes Model preformed better.



## MODEL DEPLOYMENT

As this was a school project, we weren't asked to deploy our model somewhere. If asked, we would've done that using Flask framework.

<!--
## This below block is for school's requirememt.

In getdata file we are creating a table data and storing the values from the dataset
also we are creating a dataset table to store 3 fields that we are using for prediction 
along with profit/loss. 
Then in prediction file we use bernoulie naive bayes algorithm to perform a binary 
classification, load the data from the data set that we created(dynamically) and 
based on the details of user input match for the similiar data point in dataset
give result.-->
