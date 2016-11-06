
mvn compile
mvn assembly:single

spark-submit \
--class "de.kdml.bigdatalab.spark.App" \
--master local[4] \
target/spark_boilerplate-0.0.1-SNAPSHOT-jar-with-dependencies.jar



mvn clean package
spark-submit \
--class "de.kdml.bigdatalab.spark.App" \
--master local[4] \
target/spark_boilerplate-0.0.1-SNAPSHOT.jar