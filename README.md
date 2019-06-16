# Rundeck GCP Cloud Storage Log Storage Plugin
This is a plugin for [Rundeck](http://rundeck.org) that uses [GCP Cloud Storage](https://cloud.google.com/storage/docs/) to store execution
log files.  
Based heavily on the S3 log storage plugin: https://github.com/rundeck-plugins/rundeck-s3-log-plugin

## Build

    ./gradlew clean build

## Install

Copy the `rundeck-gcp-storage-log-plugin-x.y.jar` file into the `libext` directory inside your Rundeck installation.  
Enable the ExecutionFileStorage provider named `org.rundeck.gcp-storage` in your `rundeck-config` file:  

    rundeck.execution.logs.fileStoragePlugin=org.rundeck.gcp-storage
    
## GCP Credentials
The plugin uses the GCP Client SDK library which requires valid credentials with the correct permissions to read and write to the specified Storage bucket. How to specify credentials is covered here: https://cloud.google.com/docs/authentication/production

## Configuration
The plugin only requires the name of the bucket to be specified via the `bucket` property. This can be specified in `framework.properties` as follows:  

    framework.plugin.ExecutionFileStorage.org.rundeck.gcp-storage.bucket=my-rundeck-bucket
    
