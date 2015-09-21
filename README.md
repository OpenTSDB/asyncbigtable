# AsyncBigTable 

This is an HBase library intended to work as a drop in replacement for the
fantastic AsyncHBase library and integrate OpenTSDB with Google Bigtable.

This library is using the Apache HBase 1.0 API linking the Google Bigtable
libraries.

Please note that the library is still under development and it was never meant
to replace AsyncHBase when running with an HBase backend or be a general
purpose HBase library.


## Build instructions


Maven has been adopted as the building tool for this project. To produce jar file, run 

    mvn clean package

