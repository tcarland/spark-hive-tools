#!/bin/bash
#
#  Custom jar building for the test classes
#

( jar -cvf target/spark-hive-tools-test-0.1.8.jar -C target/classes . -C target/test-classes . )


