# CrateDB + Apache Spark

CrateDB is a distributed SQL database system which is capable of managing huge amounts of data in real time.
CrateDB provides an easy, horizontal scalable and system independent solution built for IoT applications.
This project explains the necessary steps to combine CrateDB with Apache Spark, a lightning fast cluster system which offers a variety operations for BigData.
A really poplar feature of Spark is machine learning, which will be covered here.
This project explains the necessary steps to apply machine learning algorithms on big data.
For storage and ingestion, CrateDB is the software to store and query an incredible amount of data in real time.
For machine learning, Apache Spark is the quick and easy way to produce machine learning models.


Since the theory may be complex at first sight, a simple use case helps to explain the theory behind.

## Requirements

To get your hands dirty you will need:
* CrateDB cluster (either local e.g. via `docker-compose` or remote)
* Apache Spark cluster (either local or remote)
* A dataset which you want to use for machine learning e.g. twitter tweets
* An idea what you want to accomplish with that dataset e.g. language identification of text

## Use Case

The program is a language identifier, where the input is a custom text and the outcome is the predicted language.

e.g. When entering "Today is a good day!" program should identify the text as english language.

The training data for language prediction consists of a custom dataset of twitter tweets and their corresponding language.

## Raw Dataset

For this example, the used dataset are twitter tweets, which can be easily inserted into CrateDB by one click.
Note that the receiving of the dataset requires a Twitter account.

Simply navigate to Crate Admin UI (hostname:4200/#/help) and hit `import tweets for testing`

![alt text][import_tweets]

After authorization, crate will import a few tweet messages into a table named `tweets`

For importing other datasets, please see [the documentation](https://crate.io/docs/crate/guide/index.html)

## Idea

For the next steps you need to define what you actually want to achieve with this dataset. Also, it is a good idea to do some research about the situation before.

---

For this use case, the main goal is to identify the language of a given text using a machine learning model.

## Preparations

Almost no dataset is already in a usable format for machine learning. These datasets need some preparations beforehand. Preparations depend on the state of the dataset

* does the dataset contain the all the essential features?
* is the dataset labeled? (only needed for supervised machine learning)

---

The desired state of the data for training looks something like this:
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

To get to this state the training data needs to be cleaned, filtered, and labeled. 
After these transformations the training data is prepared and ready for machine learning.

## Transformations

A [transformation][definition_transformation] basically is the process of adding, editing, removing, combining parts of
one or more column(s) (also called feature(s)) of a dataset and storing the new value in a new feature.
In a pipeline all defined transformations are applied to each data record of the dataset in the order they were defined.

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

## Labeling

For [supervised machine learning][definition_supervised_machine_learning] the algorithm needs to know the correct answer of a given input.
[Unsupervised machine learning][definition_unsupervised_machine_learning] does not need to have labels.

---

An easy way to provide this label for this use case, is the usage of a [language detection library][language_library].

After this step, the training data is now cleaned and labeled and ready for supervised machine learning.

## Machine Learning

### Feature preparation

[Features][definition_features] are one or more characteristics of a situation which are used as a input parameters of machine learning. After training a model, this model tries to use these features to get to a solution.
For machine learning, features are usually a set of numbers.
But since the current features are texts, a few steps are needed to get to numbers.

#### Tokenizer + N-Gram

[Tokenization][definition_tokenization] in terms of language processing is the process of splitting text into smaller segments. These segments can either be words, silblings, sentences or single characters (whitespace included).

---

The first language identification approach which comes to peoples minds is the learning of every word in every language. This is a bad approach since there are a lot of words. A better method (which is used here) is the usage of n-grams. [Read more about N-Gram method][ngram_method].

For this project, the text will be split into lowercase characters, which are then combined to groups of size n, where n is a numeric value.
These groups are then used further. 

#### HashingTF

Unfortunately, Spark is not capable of machine learning using texts. These texts need to be transformed into numbers, where the easiest way is the use of a hashing function.

### Label preparation

[Labels][definition_labels] are the solution of a given problem.
But since machine learning can only be applied on numeric values, Spark provides two good solutions to transform texts into indices and back.

#### String Indexer 

# CONTINUE HERE

#### Index To String

### Machine Learning Algorithm

# Pipeline

# Tuning

# Apps

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

[import_tweets]: import_tweets.png
[definition_transformation]: https://spark.apache.org/docs/latest/ml-pipeline.html#main-concepts-in-pipelines
[definition_supervised_machine_learning]: https://en.wikipedia.org/wiki/Supervised_learning
[definition_unsupervised_machine_learning]: https://en.wikipedia.org/wiki/Unsupervised_learning
[definition_features]: https://stackoverflow.com/a/40899529
[definition_tokenization]: https://en.wikipedia.org/wiki/Lexical_analysis#Tokenization
[language_library]: https://github.com/shuyo/language-detection
[ngram_method]: https://stats.stackexchange.com/a/144903
[definition_labels]: https://stackoverflow.com/a/40899529
