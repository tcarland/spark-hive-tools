
export SPARK_HIVETOOLS_VERSION="0.2.1"
export SPARK_VERSION="2.4.2"
export KAFKA_VERSION="0-10_2.12"

APP_JAR="ipflow-consumer-$SPARK_HIVETOOLS_VERSION-jar-with-dependencies.jar"
KAFKA_JAR="$HOME/.m2/repository/org/apache/spark/spark-sql-kafka-${KAFKA_VERSION}/\
${SPARK_VERSION}/spark-sql-kafka-${KAFKA_VERSION}-${SPARK_VERSION}.jar"
