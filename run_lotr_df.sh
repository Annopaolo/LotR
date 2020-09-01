spark-submit \
--packages com.johnsnowlabs.nlp:spark-nlp_2.11:2.5.5 \
lotr_2.11-0.1.jar \
df \
s3://lotr-analysis-try/LordOfTheRingsBook.json \
s3://lotr-analysis-try/analyze_sentiment_en_2.4.0_2.4_1580483464667/


