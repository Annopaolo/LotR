# LotR analysis

A data analysis project for the 2019-20 "Scalable and Cloud Programming" 
course @ University of Bologna. 
The goal is to classify a selection of characters from J.R.R. Tolkien's "Lord of the Rings"
using a simple combination of sentiment analysis and tf-idf.

##Description
Sentiment analysis is performed using [Spark-NLP](https://github.com/JohnSnowLabs/spark-nlp)
and tf-idf is actually programmed. In order to double-check the result, it is possible to run
the analysis using Spark DataFrames or Spark RDDs. The latter is a bit faster and more in the
spirit of the course.

Book data are loaded from a Parquet file, then the Spark-NLP pipeline performs the sentiment
analysis on each sentence giving in output a binary classification. After this, 
sentences without character names are filtered out and for each name, df is 
computed. Then tf is computed for each sentence, and the results merged together.
Both pure tf-idf and a weighted tf-idf are displayed, the latter computed 
using sentence sentiment as weight.
The output is both logged at the end of execution and saved to a local file.

The project was deployed on AWS EMR using a m4.xlarge cluster with 1 master
and 2 core nodes. Relevant files, such as the book and the sentiment pipeline
were stored in an S3 bucket. 

##Running the project
To build the project, use:
```
sbt package
```
Assuming the configuration described at the end of the previous section,
the analysis can be run via SSH to the master node using the following command:

```
spark-submit \
--packages com.johnsnowlabs.nlp:spark-nlp_2.11:2.5.5 \
<the built jar> \ 
[df | rdd] \ 
<location of the parquet file> \
<location of the sentiment pipeline> \
<local output file location> 
```

Expected result:
```
There are 29507 documents to be considered.
-----------tfidf---------------
Bombadil : 218,4885
Galadriel : 423,2404
Smeagol : 574,8724
Sauron : 664,1040
Treebeard : 675,1724
Elrond : 806,8721
Saruman : 1089,3175
Boromir : 1161,7833
Faramir : 1206,3264
Bilbo : 1220,7761
Legolas : 1388,1415
Gollum : 1470,9466
Gimli : 1521,2633
Merry : 2014,1437
Pippin : 2371,7749
Aragorn : 2531,0008
Gandalf : 3366,4101
Sam : 3580,7971
Frodo : 4901,1013

-----------tfidf*sentiment---------------
Frodo : -1323,5675
Sam : -975,3409
Pippin : -639,6244
Gandalf : -571,6545
Gollum : -471,9860
Merry : -441,5138
Bilbo : -416,5001
Aragorn : -406,2100
Faramir : -346,7578
Boromir : -309,8089
Sauron : -254,5732
Saruman : -251,3810
Legolas : -227,4881
Smeagol : -203,2377
Elrond : -174,0312
Treebeard : -154,9576
Gimli : -131,6914
Galadriel : -108,8332
Bombadil : 54,6221
```