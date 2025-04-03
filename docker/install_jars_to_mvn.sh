#!/bin/bash

# Install copied JARs from the previous builds so maven can find them.
#
# Precondition: you have copied JARs of our local dependencies from another build into the current image
# Postcondition: JARs have been installed in the local maven repo so that mvn install can find them
if [ "$1" = "emap-interchange" ]
then
  mvn install:install-file -Dfile=/app/emap-interchange/target/emap-interchange-2.7.jar -DgroupId=uk.ac.ucl.rits.inform -DartifactId=emap-interchange -Dversion=2.7 -Dpackaging=jar
  mvn install:install-file -Dfile=/app/emap-interchange/target/emap-interchange-2.7-sources.jar -DgroupId=uk.ac.ucl.rits.inform -DartifactId=emap-interchange -Dversion=2.7 -Dpackaging=jar -Dclassifier=sources
  mvn install:install-file -Dfile=/app/emap-interchange/target/emap-interchange-2.7-tests.jar -DgroupId=uk.ac.ucl.rits.inform -DartifactId=emap-interchange -Dversion=2.7 -Dpackaging=jar -Dclassifier=tests
elif [ "$1" = "emap-star" ]
then
  mvn install:install-file -Dfile=/app/emap-star/emap-star-annotations/target/emap-star-annotations-2.7.jar -DgroupId=uk.ac.ucl.rits.inform -DartifactId=emap-star-annotations -Dversion=2.7 -Dpackaging=jar
  mvn install:install-file -Dfile=/app/emap-star/emap-star/target/emap-star-2.7.jar -DgroupId=uk.ac.ucl.rits.inform -DartifactId=emap-star -Dversion=2.7 -Dpackaging=jar
  mvn install:install-file -Dfile=/app/emap-star/emap-star/target/emap-star-2.7-sources.jar -DgroupId=uk.ac.ucl.rits.inform -DartifactId=emap-star -Dversion=2.7 -Dpackaging=jar -Dclassifier=sources
else
  echo "Did not recognise argument $1"
  exit 1
fi
