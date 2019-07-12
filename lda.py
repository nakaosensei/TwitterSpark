from __future__ import print_function
import pandas as pd
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from nltk import tokenize
from nltk.corpus import stopwords
import re as re
import os
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf, col, date_format
import pyspark.sql.types as T
from pyspark.ml.feature import CountVectorizer, CountVectorizerModel, IDF, RegexTokenizer, StopWordsRemover
from pyspark.ml.clustering import LDA, LocalLDAModel
from pyspark.ml import Pipeline
from nltk.corpus import stopwords
from nltk.stem.snowball import SnowballStemmer
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
sns.set_style("whitegrid")
import pandas as pd
import re
import nltk
from pyspark.ml.feature import StopWordsRemover,Tokenizer, RegexTokenizer, CountVectorizer, IDF
from pyspark.sql.functions import udf, col, size, explode, regexp_replace, trim, lower, lit
from pyspark.sql.types import ArrayType, StringType, DoubleType, IntegerType, LongType
from pyspark.ml.clustering import LDA
import pyLDAvis
import string



def format_data_to_pyldavis(df_filtered, count_vectorizer, transformed, lda_model,num_terms):
    xxx = df_filtered.select((explode(df_filtered.words_filtered)).alias("words")).groupby("words").count()
    word_counts = {r['words']:r['count'] for r in xxx.collect()}
    word_counts = [word_counts[w] for w in count_vectorizer.vocabulary]


    data = {'topic_term_dists': np.array(lda_model.topicsMatrix().toArray()).T, 
            'doc_topic_dists': np.array([x.toArray() for x in transformed.select(["topicDistribution"]).toPandas()['topicDistribution']]),
            'doc_lengths': [r[0] for r in df_filtered.select(size(df_filtered.words_filtered)).collect()],
            'vocab': count_vectorizer.vocabulary,
            'term_frequency': word_counts,
            'R' : num_terms}

    return data

def filter_bad_docs(data):
    bad = 0
    doc_topic_dists_filtrado = []
    doc_lengths_filtrado = []

    for x,y in zip(data['doc_topic_dists'], data['doc_lengths']):
        if np.sum(x)==0:
            bad+=1
        elif np.sum(x) != 1:
            bad+=1
        elif np.isnan(x).any():
            bad+=1
        else:
            doc_topic_dists_filtrado.append(x)
            doc_lengths_filtrado.append(y)

    data['doc_topic_dists'] = doc_topic_dists_filtrado
    data['doc_lengths'] = doc_lengths_filtrado



def update_lda(lang = "portuguese"):    
    file_location = "files/news/news_{}.csv".format(lang)
    sc = SparkContext(appName="PythonStreamingReceiver")
    sqlc = SQLContext(sc)

    
    df = sqlc.read.csv(file_location, header=True)

    news = df.rdd.map(lambda x: x['title']).filter(lambda x: x is not None)
    headlines = news.zipWithIndex()
    data = sqlc.createDataFrame(headlines,["headlines",'index'])

    personal_stops = ["http","www","html","https","id","bolsonaro","none","yahoo","reuters","brazil","name","news"]
    stopWords = stopwords.words(lang)
    stopWords.extend(personal_stops)

    removePunct = udf(
        lambda s: re.sub(r'[^A-Za-záàâãéèêíïóôõöúçñÁÀÂÃÉÈÍÏÓÔÕÖÚÇÑ]|[0-9]', r' ', s).strip().lower(), T.StringType())

    data_norm = data.withColumn("text", removePunct(data.headlines))
    tokenizer = RegexTokenizer(inputCol="text", outputCol="words",
                            gaps=True, pattern=r'\s+', minTokenLength=4)
    df_tokens = tokenizer.transform(data_norm)
    removeStop=udf(lambda word: [x for x in word if x not in stopWords])
    df_tokens=df_tokens.withColumn('noStopWords',removeStop(df_tokens['words']))
    df_tokens.printSchema()

    label_udf = udf(lambda x: x, T.ArrayType(T.StringType())) 

    df_tokens=df_tokens.withColumn("words_filtered",label_udf(df_tokens.noStopWords))
    df_tokens.show()
    cv = CountVectorizer(inputCol="words_filtered", outputCol="features", minDF = 0.01, maxDF = 0.98)
    cvmodel = cv.fit(df_tokens)

    result_cv = cvmodel.transform(df_tokens)
    idf = IDF(inputCol="features", outputCol="features_tfidf")
    idfModel = idf.fit(result_cv)
    result_tfidf = idfModel.transform(result_cv)

    df_training, df_testing = result_tfidf.randomSplit([0.8, 0.2], 1)
    num_topics = 8
    max_iterations = 50
    lda = LDA(k=num_topics, maxIter=max_iterations)
    ldaModel = lda.fit(result_tfidf)
    transformed = ldaModel.transform(result_tfidf)
    lpt, lp = ldaModel.logPerplexity(df_testing), ldaModel.logPerplexity(df_training)
    
    num_topics = int(lpt+1)
    num_terms = 15
    
    # FORMAT DATA AND PASS IT TO PYLDAVIS
    data = format_data_to_pyldavis(df_tokens, cvmodel, transformed, ldaModel, num_terms)
    filter_bad_docs(data)
    py_lda_prepared_data = pyLDAvis.prepare(**data)
    pyLDAvis.save_html(py_lda_prepared_data, 'files/viz/lda.html')



    #Topics and top-weighted terms
    topics = ldaModel.describeTopics(maxTermsPerTopic = num_terms)
    vocabArray = cvmodel.vocabulary

    ListOfIndexToWords = udf(lambda wl: list([vocabArray[w] for w in wl]))
    FormatNumbers = udf(lambda nl: ["{:1.4f}".format(x) for x in nl])

    topics.select(ListOfIndexToWords(topics.termIndices).alias('words')).show(truncate=False, n=num_topics)
    topics.select(FormatNumbers(topics.termWeights).alias('weights')).show(truncate=False, n=num_topics)
    print("Perplexity on testing and training data: " + str(lp) + ',' + str(lpt))

