Build: 
Pre-requisite: maven 3 (or higher

The project can be built with the following command:
mvn clean package

Considering the present working directory is that of the project directory, the job can be submitted as [ flink run -c com.acmecorp.jobs.AcmeReports target/exercise-skeleton-1.0-SNAPSHOT.jar --all hdfs://localhost:9000/acme/activities/report/all --more hdfs://localhost:9000/acme/activities/report/more --less hdfs://localhost:9000/acme/activities/report/less --checkpoint hdfs://localhost:9000/acme/activities/checkpoint ], where

a.       “--all" is followed by the path to output unfiltered events per user id in the form of (<user id>,<number of events>)

b.       “--more" is followed by the path to output user ids with 10 or more events in the current window

c.       “--less" is followed by the path to output user ids with less than 10 events in the current window

d.       “--checkpoint" is followed by the path to a folder where checkpointed state can be saved for fault-tolerance purposes
