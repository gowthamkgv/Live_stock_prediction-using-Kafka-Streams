from pyspark import SparkContext, SQLContext
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.feature import StringIndexer
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import NGram, VectorAssembler
from pyspark.ml.feature import ChiSqSelector
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import PipelineModel


#split data to train model
sc = SparkContext()
spark = SQLContext(sc)
df =  spark.read.csv("/Users/chaitanyavarmamudundi/Desktop/imdb_labelled.txt", sep="\t")
df = df.selectExpr("_c0 as text", "_c1 as label")
df = df.withColumn("label", df["label"].cast("int"))
df = df.dropna()
#df.show(100)
splitDF = df.randomSplit([8.0,2.0],32)
train_set = splitDF[0]
test_set = splitDF[1]

#preprocessing the data to perform NLP
def preprocess(inputCol=["text","label"], n=4):
    tokenizer = [Tokenizer(inputCol="text", outputCol="words")]
    remover = [StopWordsRemover(inputCol="words", outputCol="filtered")]
    ngrams = [
        NGram(n=i, inputCol="filtered", outputCol="{0}_grams".format(i))
        for i in range(1, n + 1)
    ]

    cv = [
        CountVectorizer(vocabSize=2**14,inputCol="{0}_grams".format(i),
            outputCol="{0}_tf".format(i))
        for i in range(1, n + 1)
    ]
    idf = [IDF(inputCol="{0}_tf".format(i), outputCol="{0}_tfidf".format(i), minDocFreq=2) for i in range(1, n + 1)]

    assembler = [VectorAssembler(
        inputCols=["{0}_tfidf".format(i) for i in range(1, n + 1)],
        outputCol="rawFeatures"
    )]
    label_stringIdx = [StringIndexer(inputCol = "label", outputCol = "labels")]
    selector = [ChiSqSelector(numTopFeatures=2**14,featuresCol='rawFeatures', outputCol="features")]
    lr = [LogisticRegression(maxIter=1000)]
    return Pipeline(stages=tokenizer + remover + ngrams + cv + idf+ assembler + label_stringIdx+selector + lr)
#saving pipeline steps of execute
pipeline_load = PipelineModel.load("/Users/chaitanyavarmamudundi/Desktop/pipeLineModel")
predictions = pipeline_load.transform(test_set) #put dataframe for testing here
int(predictions.collect()[-1]['prediction']) #prediction

#finding the accuracy of the model.
accuracy = predictions.filter(predictions.label == predictions.prediction).count() / float(test_set.count())
evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction")
roc_auc = evaluator.evaluate(predictions)
print("Accuracy Score: ", accuracy)
print("ROC-AUC: {0:.4f}", roc_auc)

#loading pipeline and predicting the accuracy of new data.
predictions = pipeline_load.transform(ddf)
int(predictions.collect()[-1]['prediction'])
