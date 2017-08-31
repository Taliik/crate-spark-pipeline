# CrateDB + Apache Spark

This project explains the necessary steps to apply machine learning algorithms on big data.
For storage and ingestion, CrateDB is the software to store and query an incredible amount of data in real time.
For stream based machine learning, Apache Spark is the quick and easy way to produce machine learning models.

## Use Case

For simple demonstration, the use case will be a language recognition model for simple text inputs.
The model itself learns from a custom dataset of twitter tweets and a corresponding language.

## Requirements

* CrateDB cluster (either local e.g. via `docker-compose` or remote)
* Apache Spark cluster (either local or remote)
* A dataset
* An idea what you want to accomplish with that dataset

## Raw Dataset

For the receiving of the dataset you will need a Twitter account.
Simply navigate to crate-admin-ui (hostname:4200/#/help) and hit `import tweets for testing`

![alt text][import_tweets]

After authorization, crate will import a few tweet messages into a table named `tweets`

For importing other datasets, please see [the documentation](https://crate.io/docs/crate/guide/index.html)

## Idea

For the next steps you need to define what you actually want to achieve with this dataset.

For this use case, the main goal is to identify the language of a given text using a machine learning model.

## Transformations

A [transformation][definition_transformation] basically is the process of adding, editing, removing, combining parts of
one or more column(s) (also called feature(s)) of a dataset and storing the new value in a new feature.
All defined transformations are applied to each data record of the dataset in the order they were defined.

When having a look at our twitter data, we recognize that there are some segments of `text` which don't provide
information for language detection.
This includes:

* the '`RT @username:`' of retweets -> drop it
* other user mentionings e.g. '`@username`' -> drop it
* Hashtags -> drop it
* E-mail-addresses -> drop it
* URIs -> drop it
* Emojis -> drop it

To provide reliable texts to the language detection algorithm, it is also advised to set a minimum text length.
The remaining text is the text used as training data.

## Labeling

For [supervised machine learning][definition_supervised_machine_learning] the algorithm needs to know the correct answer
of a given input. To provide that in an easy way, a language detection library analyzes the dataset and adds a language
classifier.


## Machine Learning

Now that the tweets are clean labeled texts, machine learning can be applied on this dataset.

[import_tweets]: import_tweets.png
[definition_transformation]: https://spark.apache.org/docs/latest/ml-pipeline.html#main-concepts-in-pipelines
[definition_supervised_machine_learning]: https://en.wikipedia.org/wiki/Supervised_learning

## Apps

## Benchmarks

select correct.c, whole.w, (correct.c::double/whole.w::double) from (select count(*) as c from predicted_tweets where label=prediction) as correct, (select count(*) as w from predicted_tweets) as whole
