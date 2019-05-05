#!/bin/bash
#
#  Custom jar building for the test classes
#

( jar -cvf target/spark-hive-tools-test-0.2.2.jar -C target/classes . -C target/test-classes . )
