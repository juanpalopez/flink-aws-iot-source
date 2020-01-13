# FLINK AWS IoT Core Source sample

This a is a sample project to consume data from AWS IoT Core using Flink. 
It may be helpful if you want use it on a Kinesis Analytics Application, which is serverless. 
Since this was developed before **ReInvent 2019** it uses Flink v1.6. Now v1.8 is supported.
You may change the pom.xml dependency

* You will need upload your certificates on a S3 bucket and set them on your Streaming Job

You may find a sample Streaming Job showing how to use it.
```
src/main/java/iotCore/awsIotCore/AwsIotCoreSource.java
```