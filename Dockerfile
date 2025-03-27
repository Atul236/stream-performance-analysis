FROM quay.io/astronomer/astro-runtime:12.7.1


RUN pip install pandas boto3


# Install Java (default-jdk)


# Set JAVA_HOME environment variable
# ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
# ENV PATH=$JAVA_HOME/bin:$PATH

# Install PySpark and dependencies
# Upgrade pip before installing dependencies
# Upgrade setuptools
# Upgrade pip, setuptools, and wheel
# RUN pip install --upgrade pip setuptools wheel

# Install dependencies from the pre-downloaded directory
# COPY dependencies /tmp/dependencies
# RUN pip install /tmp/dependencies/*



# Add Hadoop-AWS JAR and AWS SDK JAR for MinIO integration
# ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar $AIRFLOW_HOME/jars/
# ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar $AIRFLOW_HOME/jars/

# # Set Spark JAR directory
# ENV SPARK_CLASSPATH=$AIRFLOW_HOME/jars/

# USER astro
