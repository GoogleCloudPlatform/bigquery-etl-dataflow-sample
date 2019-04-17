# Performing ETL into Big Query Tutorial Sample Code

This is the sample code for the Performing ETL from a Relational Database into BigQuery using Dataflow.  The tutorial explains how to ingest highly normalized (OLTP database style) data into Big Query using DataFlow. To understand this sample code it is recommended that you review the [Apache Beam programming model](https://beam.apache.org/documentation/programming-guide/). 

This tutorial relies on the [musicbrainz dataset](https://musicbrainz.org/doc/MusicBrainz_Database). 

Note that this tutorial assumes that either:
* you are using Cloud Shell
* you have Java 8, maven and [Google Cloud SDK](https://cloud.google.com/sdk/docs/) installed.  

It also requires that you have a project, dataflow staging bucket and big query dataset already created. 


## Overview

The repository consists of the following Java classes: 

1. com.google.cloud.bqetl.BQETLSimple - Simple pipeline for ingesting musicbrainz artist recording data as a flat table of artist's recordings.
2. com.google.cloud.bqetl.BQETLNested - Revision of the simple pipeline that nests the artist's recordings as a repeated record inside each Big Query table row which pertains to an artist.
3. com.google.cloud.bqetl.mbdata.MusicBrainzDataObject - a general purpose object to represent a row of MusicBrainz data
4. com.google.cloud.bqetl.mbdata.MusicBrainzTransforms - the library of functions that implements the transforms used in the above pipelines and allows them to be reused
5. com.google.cloud.bqetl.json.JSONReader - class for converting a JSON representation of a MusicBrainz tow into a MusicBrainzDataObject
6. com.google.cloud.bqetl.options.BQETLOptions - the options for the BQETL pipelines
7. com.google.cloud.bqetl.JSONReaderTest - test for the JSONReader
8. com.google.cloud.bqetl.mbdata.MusicBrainzTransformsTest - tests the transforms library

The repository consists of the following scripts and resources: 

1. src/test/resources data files for the test classes 
1. dataflow-staging-policy.json - a policy for expiring objects in the bucket used for staging the dataflow jobs
1. run.sh - example script for running the pipelines using maven
1. pom.xml - maven build script 
 

## Quickstart: Running the ETL Pipelines

NOTE: For more detail instructions, and pipeline details, see the [full tutorial on Google Cloud Solutions](https://cloud.google.com/solutions/performing-etl-from-relational-database-into-bigquery). 

1. Follow the instructions to set up your project and service account:

   [Dataflow using Java and Apache Maven: Before you begin](https://cloud.google.com/dataflow/docs/quickstarts/quickstart-java-maven#before-you-begin)

1. Set your environment variables.  

   For example: 

     ```shell
     export PROJECT_ID=[YOUR_PROJECT_ID]
     export ZONE=[CHOOSE_AN_APPROPRIATE_ZONE] # e.g. us-east1-c
     export STAGING_BUCKET=${PROJECT_ID}-etl-staging-bucket
     export DATASET=musicbrainz
     
     ```

1. First, run the `simple` pipeline using the script `run.sh`:

     ```shell
     export DESTINATION_TABLE=recordings_by_artists_dataflow
     ./run.sh simple
     ```
     
   when this pipeline finishes, you can review the results in the BigQuery table: `recordings_by_artists_dataflow`

1. Then, run the `nested` pipeline using the script `run.sh`:

     ```shell
     export DESTINATION_TABLE=recordings_by_artists_dataflow_nested
     ./run.sh nested
     ```
    when this pipeline finishes, you can review the results in the BigQuery table: `recordings_by_artists_dataflow_nested`

## Contact Us

We welcome all usage-related questions on [Stack Overflow](http://stackoverflow.com/questions/tagged/google-cloud-dataflow)
tagged with `google-cloud-dataflow`.

Please use [issue tracker](https://github.com/GoogleCloudPlatform/bqii-dataflow/issues)
on GitHub to report any bugs, comments or questions regarding SDK development.

## More Information

* [Google Cloud Dataflow](https://cloud.google.com/dataflow/)
* [Apache Beam programming model](https://beam.apache.org/documentation/programming-guide/).
* [Java API Reference](https://cloud.google.com/dataflow/java-sdk/JavaDoc/index)
