#!/bin/bash
#
#  Custom jar building for the test classes
#

echo "Building Test Jar..."

( jar -cvf target/spark-hive-tools-test-0.2.8.jar -C target/classes . -C target/test-classes . )

exit $?
