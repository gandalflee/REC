#!/bin/bash

#guess u like step2: data mining
input="/RecommenderSystem/JiLinSMEPSP/RecommenderEngine/Service/GuessULike/UserPreference"
output="/RecommenderSystem/JiLinSMEPSP/RecommenderEngine/Service/GuessULike/CFOutput"
tmpdir="/RecommenderSystem/JiLinSMEPSP/RecommenderEngine/Service/GuessULike/tmp"
similarityClassname="org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.LoglikelihoodSimilarity"
numRecommendations="10"

#converte and final recommendations
converter_jar="/home/casliyang/Myfiles/gul-itemcf-hadoop-converter.jar"
converter_class="cn.fulong.bigdata.recommendersystem.recommenderengine.guessulike.ItemCFOutputConverter"
converted_data_path="/RecommenderSystem/JiLinSMEPSP/RecommenderEngine/Service/GuessULike/ConvertedRec"

#******similarity measure classes******
#org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.CityBlockSimilarity
#org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.CooccurrenceCountSimilarity
#org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.CosineSimilarity
#org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.CountbasedMeasure
#org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.EuclideanDistanceSimilarity
#org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.LoglikelihoodSimilarity
#org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.PearsonCorrelationSimilarity
#org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.TanimotoCoefficientSimilarity

#---------------------------------------------------------
hadoop jar $MAHOUT_HOME/mahout-core-0.9-cdh5.2.0-job.jar org.apache.mahout.cf.taste.hadoop.item.RecommenderJob --input ${input} --output ${output} --tempDir ${tmpdir} --similarityClassname ${similarityClassname} --numRecommendations ${numRecommendations}

hadoop jar ${converter_jar} ${converter_class} ${output} ${converted_data_path}

hadoop fs -rm -r ${output}
hadoop fs -rm -r ${tmpdir}
