# basic-data-maps

Right now Java and scala service tables are mapped 

To use pyspark mappings first run 

python3 -m pip install pyspark --break-system-packages

https://www.machinelearningplus.com/pyspark/pyspark-connect-to-mysql/


# Run below command to install Apache Spark
brew install apache-spark
# Start Spark
spark-shell
# Once you get Scala> prompt, run below command to verify it returns "Hello World!"
scala> val s = "Hello World!"
# If you get error, make sure you set SPARK_HOME and add path as below in your profile
export SPARK_HOME=/usr/local/Cellar/apache-spark/3.1.2/libexec
export PYTHONPATH=/usr/local/Cellar/apache-spark/3.1.2/libexec/python/:$PYTHONP$
source ~/.bash_profile
# Run below command to start all the services
/usr/local/Cellar/apache-spark/3.5.4/libexec/sbin/start-all.sh
# Navigate to http://localhost:8080/ to access Spark Master webpage
# Navigate to http://localhost:4040/ to access Spark Application webpage
# Remember to cleanup and stop all of the services
/usr/local/Cellar/apache-spark/3.5.4/libexec/sbin/stop-all.sh


