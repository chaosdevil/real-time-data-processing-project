# real-time-data-processing-project
Real time data processing using Kafka and Apache Spark

--------------------------------- for local machine ---------------------------------

# run project
`spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 spark-streaming.py 18.211.252.152 9092 real-time-project`



--------------------------------- for ec2 instance (cloudera manager) ---------------------------------

# run project (setting up default kafka to 0.10 is required on spark configuration)
`spark2-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2 spark-streaming.py 18.211.252.152 9092 real-time-project`

# access and move files
`ssh -i "cloudera.pem" ec2-user@ec2-34-239-166-22.compute-1.amazonaws.com`

`scp -i "cloudera.pem" real-time-project/spark-streaming.py ec2-user@ec2-34-239-166-22.compute-1.amazonaws.com:~/real-time-project`

`scp -i "cloudera.pem" ec2-user@ec2-34-239-166-22.compute-1.amazonaws.com:~/real-time-project/spark-streaming.py C:\Users\yoksu\Downloads\real-time-project`

`scp -i "cloudera.pem" ec2-user@ec2-34-239-166-22.compute-1.amazonaws.com:~/real-time-project/op1 C:\Users\yoksu\Downloads\submission\`
