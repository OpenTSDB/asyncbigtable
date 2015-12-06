# AsyncBigTable 

This is an HBase library intended to work as a drop in replacement for the
fantastic AsyncHBase library and integrate OpenTSDB with Google Bigtable.

This library is using the Apache HBase 1.0 API linking the Google Bigtable
libraries.

Please note that the library is still under development and it was never meant
to replace AsyncHBase when running with an HBase backend or be a general
purpose HBase library.


## Basic Installation

Contrary to the original asynchbase library, asyncbigtable has adopted Maven
as the building tool for this project.

To produce jar file simply run:

    mvn clean package



