# spark-hive-tools runtime configuration
#

export SPARK_HIVETOOLS_VERSION="0.6.1"
export SPARK_VERSION="3.3.1"
export KAFKA_VERSION="0-10_2.13"

APP_JAR="target/spark-hive-tools-$SPARK_HIVETOOLS_VERSION-jar-with-dependencies.jar"
KAFKA_JAR="$HOME/.m2/repository/org/apache/spark/spark-sql-kafka-${KAFKA_VERSION}/\
${SPARK_VERSION}/spark-sql-kafka-${KAFKA_VERSION}-${SPARK_VERSION}.jar"
