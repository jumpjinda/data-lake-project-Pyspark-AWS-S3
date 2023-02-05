# data-lake-project-Pyspark-AWS-S3

This project was implemented on a local machine with Pyspark read JSON files from local machine and tranformation Pyspark dataframe to tables then save files to S3.

## Summary

This project combines song listen log files with song metadata to facilitate analytics. JSON data is read from local machine and processed using Apached Spark with the Python API. The data is organized into a star schema with fact and dimension tables. Analytics queries on the ` songplays` fact table are straightforward, and additional fields can be easily accessed in the four dimension tables `users`, `songs`, `artists`, and `time`. A star schema is suitable for this application since denormalization is easy, queries can be kept simple, and aggregations are fast.

## Run

1. Fill your `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` to dl.cfg
2. Create a S3 bucket and replace the `output_data` variable in the `main()` function with `s3a://<bucket name>/`
