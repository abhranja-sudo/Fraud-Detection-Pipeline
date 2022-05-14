from pyspark.ml.feature import VectorAssembler


def scaleData(test, features):
    assembler = VectorAssembler(inputCols=features, outputCol="feature")
    test_df = assembler.transform(test.to_spark())
    return test_df