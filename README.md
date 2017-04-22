# AsyncBigtable 

[![Travis CI status][travis-shield]][travis-link]
[![Maven][maven-shield]][maven-link]
[![Stack Overflow][stackoverflow-shield]][stackoverflow-link]

This is an HBase library intended to work as a drop in replacement for the
fantastic AsyncHBase library and integrate OpenTSDB with Google Bigtable.
It is using the Apache HBase 1.0 API linking the Google Bigtable
libraries. 

This library started out as a fork of the asynchbase 1.5.0 library, therefore one may 
find code that at first sight may look strange. We are working on cleaning
up the code base and removing irrelevant dependencies.

## Basic Installation

Contrary to the original `asynchbase` library, `asyncbigtable` has adopted Maven
as the building tool for this project.

To produce the jar file, simply run:

    mvn clean package

Maven will produce the following two jar files under the `target/` directory:

1. `asyncbigtable-<version>.jar` which is the compiled jar file
2. `asyncbigtable-<version>-jar-with-dependencies.jar` which is an assembly jar containing 
all dependencies required for asyncbigtable to run with OpenTSDB. Please note that this 
jar does not include all dependencies but only the ones for OpenTSDB.

## Javadoc

Since AsyncBigtable tries to be 100% compatible with AsyncHBase, please read the 
[AsyncHBase javadoc](http://opentsdb.github.io/asynchbase/javadoc/index.html).

## Changelog

This project uses [Semantic Versioning](http://semver.org/).

### 0.3.0

- Updated dependency to com.google.cloud.bigtable:bigtable-hbase-1.2:0.9.6
- Updated dependency to protobuf-java:3.0.2
- Updated dependency to netty-all 4.1.0.Final 

### 0.2.1

- This is the first public release of the asyncbigtable library, that
started out as a fork of the asynchbase 1.5.0 library.
- Modified all HBase access operations to use the standard HBase 1.0 API calls
- Added Google Bigtable 0.2.2 dependency
- Changed project build system from Make to Maven
- Added assembly Maven plugin to build uber jar for distribution with OpenTSDB

## Disclaimer

Please note that the library is still under development and it was never meant
to replace AsyncHBase when running with an HBase backend or be a general
purpose HBase library.

<!-- references -->

[travis-shield]: https://travis-ci.org/OpenTSDB/asyncbigtable.svg
[travis-link]: https://travis-ci.org/OpenTSDB/asyncbigtable
[maven-shield]: https://img.shields.io/maven-central/v/com.pythian.opentsdb/asyncbigtable.svg
[maven-link]: https://search.maven.org/#search%7Cga%7C1%7Ccom.pythian.opentsdb
[stackoverflow-shield]: https://img.shields.io/badge/stackoverflow-opentsdb-green.svg
[stackoverflow-link]: https://stackoverflow.com/questions/tagged/opentsdb
