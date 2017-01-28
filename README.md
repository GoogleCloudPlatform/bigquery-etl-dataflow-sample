# Performing ETL into Big Query Tutorial Sample Code

This is the sample code for the Big Query Ingestion to Insights Tutorial.  The tutorial explains how to ingest highly normalized (OLTP database style) data into Big Query using DataFlow. To understand this sample code it is recommended that you review the [Google Cloud Dataflow programming model]. (https://cloud.google.com/dataflow/). 

This tutorial relies on the [musicbrainz dataset](https://musicbrainz.org/doc/MusicBrainz_Database). 

Note that this tutorial assumes that you have Java 8 and maven and [Google Cloud SDK](https://cloud.google.com/sdk/docs/) installed.  It also requires that you have a project, dataflow staging bucket and big query dataset already created. 


## Overview

The repository consists of the following Java classes: 

1. com.google.cloud.bqetl.BQETLSimple - Simple pipeline for ingesting musicbrainz artist recording data as a flat table of artist's recordings.
2. com.google.cloud.bqetl.BQETLNested - Revision of the simple pipeline that nests the artist's recordings as a repeated record inside each Big Query table row which pertains to an artist.
3. com.google.cloud.bqetl.mbdata.MusicBrainzDataObject - a general purpose object to represent a row of MusicBrainz data
4. com.google.cloud.bqetl.mbdata.MusicBrainzTransforms - the library of functions that implements the transforms used in the above pipelines and allows them to be reused
5. com.google.cloud.bqetl.json.JSONReader - class for converting a JSON representation of a MusicBrainz tow into a MusicBrainzDataObject
6. com.google.cloud.bqetl.options.BQETLOptions - the options for the BQETL pipelines
7. com.google.cloud.bqetl.JSONReaderTest - test for the JSONReader
8. com.google.cloud.bqetl.MusicBrainzTransformsTest - tests the transforms library

The repository consists of the following scripts and resources: 

1. src/test/resources data files for the test classes 
2. dataflow-staging-policy.json - a policy for expiring objects in the bucket used for staging the dataflow jobs
3. run-simple.example - example script for running the simple pipeline using maven
4. run-nested.example - example script for running the nested pipeline using maven
5. pom.xml - maven build script 
 

## Getting Started

1. copy the files run-simple.example and run-nested.example to run-simple and run-nested respectively.
'''
cp run-simple.example run-simple
cp run-nested.example run-nested 

2. Edit each file replacing the  #STAGING_BUCKET_, #PROJECT, #DATASET with the values specific to your account. 
3. Edit each file replacing #DESTINATION_TABLE with the table you want to load the denormalized data into.  Note that to preserve the table for each run you may want to use different destination tables for each of these scripts. 
4. Save the changes for each script.
5. Run either script as desired.
6. As an alternative to editing the script you can simply copy and paste the command therein to your shell replacing the aforementioned values with those specific to your project.


    
## Contact Us

We welcome all usage-related questions on [Stack Overflow](http://stackoverflow.com/questions/tagged/google-cloud-dataflow)
tagged with `google-cloud-dataflow`.

Please use [issue tracker](https://github.com/GoogleCloudPlatform/bqii-dataflow/issues)
on GitHub to report any bugs, comments or questions regarding SDK development.

## More Information

* [Google Cloud Dataflow](https://cloud.google.com/dataflow/)
* [Dataflow Concepts and Programming Model](https://cloud.google.com/dataflow/model/programming-model)
* [Java API Reference](https://cloud.google.com/dataflow/java-sdk/JavaDoc/index)
