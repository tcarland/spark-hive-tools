


all: spark-hive-tools test

spark-hive-tools:
	( mvn package )

test:
	( mvn scala:testCompile && ./src/test/resources/build-test-jar.sh )

clean:
	( mvn clean )

distclean: clean

install:
