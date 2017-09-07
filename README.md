# CrateDB + Apache Spark

[CrateDB](http://crate.io) is a distributed SQL database system which is capable of managing huge amounts of data in real time.
It is an easy, horizontal scalable, efficient and system independent solution built for IoT applications.

Though CrateDB is a pretty solid choice for managing BigData, it was not built for performing general data analysis.
This project adds this feature to CrateDB by creating a connection with Apache Spark as data processing tool.

[Apache Spark](http://spark.apache.org) is a cluster computing engine which offers lots and lots of various data operations.
All data operations are done in a distributed way, where all computations are done in RAM, which performs about 100x times faster than the previous Apache Hadoop system.
A really poplar feature of Spark is machine learning, which will also be covered here.

Since the theory may be complex at first sight, a simple use case helps to explain the theory behind.

## Requirements

To get your hands dirty on this project you will need:

* CrateDB cluster
* Apache Spark cluster
* A dataset which you want to use e.g. twitter tweets
* An idea what you want to accomplish with that dataset e.g. language identification of text

## Use Case

Every solution starts with a problem.
These articles should give an idea of the problems which can be solved with this approach.

* [Top five Spark use cases](https://www.dezyre.com/article/top-5-apache-spark-use-cases/271)
* [Spark (notable) use cases](https://jaceklaskowski.gitbooks.io/mastering-apache-spark/exercises/spark-notable-use-cases.html)
* [Introduction to Apache Spark with Examples and Use Cases](https://www.toptal.com/spark/introduction-to-apache-spark)

---

To keep everything as simple as possible, this project takes care of a rather simple task: language identification.
The general idea of this process is to take some text as input and predict the possible language as outcome.

e.g. When entering "Today is a good day!" program should identify the text as english language.

This language identification is not hardcoded into a program, which follows a bunch of rules to get to the correct language.
Instead, a machine learning algorithm trained by Twitter Tweets, takes care of the identification.

## Dataset

In the beginning a dataset is needed which shall be used later on.
This dataset could either consist of sensor data produced by industry, social media data, stock prices, etc.

For importing datasets, please refert to the [CrateDB documentation](https://crate.io/docs/crate/guide/index.html)

---

For this example, the used dataset are Twitter Tweets, which can be easily inserted into CrateDB by one click.
Note that the import of the data requires a Twitter account.

Simply navigate to Crate Admin UI (hostname:4200/#/help) and hit `import tweets for testing`.

![alt text](import_tweets.png)

After authorization, CrateDB will import a few tweet messages into a table named `tweets`.

## Idea

For the next steps an idea needs to be defined how a possible solution could look like with this dataset.

Especially when trying to solve a machine learning problem, it is a good idea to do some research about the situation beforehand.
This way it is easier to determine a suitable plan to a solution.
On the other hand it is also necessary to know if machine learning is even the right approach to solve this particular problem.
These two articles give a good overview to get started:

* [How to choose algorithms for Microsoft Azure Machine Learning](https://docs.microsoft.com/en-us/azure/machine-learning/machine-learning-algorithm-choice)
* [Essentials of Machine Learning Algorithms](https://www.analyticsvidhya.com/blog/2015/08/common-machine-learning-algorithms/)

---

For this use case, the main goal is to identify the language of a given text using a machine learning model.
After some research the language identification problem can be categorized as a [classification](https://en.wikipedia.org/wiki/Statistical_classification) problem.

## Dataset Preparations

Almost no dataset is already in a usable format for a custom situation.
These datasets need some preparations beforehand.

Preparations depend on the state of the dataset:
* Does the dataset contain the all the essential features or is some information missing?
* Is the data consistent or are there lots of errors and invalid data?

---

For this project desired state of the data for training looks something like this:

```
+===========================+===========+
|text                       |language   |
+===========================+===========+
|Today is a good day!       |en         |
+---------------------------+-----------+
|Heute ist ein schöner Tag! |de         |
+---------------------------+-----------+
|...                        |...        |
+---------------------------+-----------+
```

But when having a look at the current state, the data may not look as it should:
```
+===========================================================================================================+
| text                                                                                                      |
+===========================================================================================================+
| @wachakonochi 出番、お疲れ様でした。                                                                         |
+-----------------------------------------------------------------------------------------------------------+
| RT @selma_topsakal3: #izmirescort https://t.co/hKoJriVH6k                                                 |
+-----------------------------------------------------------------------------------------------------------+
| RT @sweet_haribo: 갑자기 앨범 하나에서 세장의 포카가 후득 떨어졌다. 삼백현이었다. 올해 운 다 썼다. https://t.co/1XeW9fLRhY   |
+-----------------------------------------------------------------------------------------------------------+
| RT @marcorubio: For all of S.Fla, all preparations evacuations should be COMPLETED by sunset on Friday.   |
+-----------------------------------------------------------------------------------------------------------+
| posição 20115 no Ranking segue-de-volta. Confira os 100 primeiros: https://t.co/VhYsyMzBHF. #1 @asciiART  |
+-----------------------------------------------------------------------------------------------------------+
| #chibalotte                                                                                               |
+-----------------------------------------------------------------------------------------------------------+
| رفرف على #الحد_الجنوبي                                                                                         |
+-----------------------------------------------------------------------------------------------------------+
| 😆😆😆                                                                                                     |
+-----------------------------------------------------------------------------------------------------------+
```

To get from current to the desired state the data needs to be cleaned, filtered, and labeled.
These operations can be done by using transformations in Spark.

### Transformations

A [transformation](https://spark.apache.org/docs/latest/ml-pipeline.html#main-concepts-in-pipelines) basically is the process of adding, editing, removing, combining parts of
one or more column(s) of a dataset and storing the outcome in a new column.
Transformations are applied in their defined order on all rows of a dataset. 

---

When having a look at the raw twitter data there are some segments of `text` which don't provide usable information for language detection.
These segments are dropped:

* the '`RT @username:`' of retweets
* other user mentionings e.g. '`@username`'
* Hashtags
* E-mail-addresses
* URIs
* Emojis 😋😉

To provide reliable texts to the language detection algorithm, it is also recommended to set a minimum text length.
After these transformations the training data is almost prepared and ready for machine learning.

### Labeling

For [supervised machine learning](https://en.wikipedia.org/wiki/Supervised_learning) the algorithm needs to know the correct answer of a given input.
[Unsupervised machine learning](https://en.wikipedia.org/wiki/Unsupervised_learning) does not need to have labels.

---

A label is still missing in the current training data. 
An easy way to provide the label for this use case is the usage of a [language detection library](https://github.com/shuyo/language-detection).
After this step, the training data has the desired format.

## Pipeline

Since the dataset finally is in the desired state the main-work-process can begin.   
This process consists of an ordered list of transformations and estimations, which are applied on the dataset. 

### Feature preparation

[Features](https://stackoverflow.com/a/40899529) are one or more characteristics of a situation which are used as a input parameters of machine learning.
After training a model, this model tries to use the features of unknown problems to get to a solution.

For machine learning, features are usually a set of numbers.
But since the current features are texts, a few steps are needed to get to numbers.

#### Tokenizer + N-Gram

[Tokenization](https://en.wikipedia.org/wiki/Lexical_analysis#Tokenization) in terms of language processing is the process of splitting text into smaller segments. These segments can either be words, silblings, sentences or single characters (whitespace included).

---

The first language identification approach which comes to peoples minds is the learning of every word in every language. This is a bad approach since there are a lot of words. A better method (which is used here) is the usage of n-grams. [Read more about N-Gram method](https://stats.stackexchange.com/a/144903).

For this project, the text will be split into lowercase characters, which are then combined to groups of size n, where n is a numeric value.
These groups are then used further. 

#### HashingTF

Unfortunately, Spark is not capable of machine learning using texts. These texts need to be transformed into numbers, where the easiest way is the use of a hashing function.

### Label preparation

[Labels](https://stackoverflow.com/a/40899529) are the solution of a given problem.
But since machine learning can only be applied on numeric values, Spark provides two good solutions to transform texts into indices and back.

#### String Indexer 

#### Index To String

### Machine Learning Algorithm

## Tuning

## Fetch Data from CrateDB using Spark

## Ingest Data into CrateDB using Spark


---





# Apps

## LearnFromTwitter

## PredictCrateData

## LearnAndPredict

## PredictLocalUserInput



# Evaluation

```
select
    correct.c as correct,
    predicted.p as predicted,
    (correct.c::double/predicted.p::double*100.0) as percent_successrate
from
    (select count(*) as c from predicted_tweets where label=prediction) as correct,
    (select count(*) as p from predicted_tweets) as predicted
```
