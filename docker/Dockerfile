FROM apache/spark:3.5.2

USER root

# ------------------------------------------------
# System Dependencies
# ------------------------------------------------
RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    groff \
    less \
    nano \
    && rm -rf /var/lib/apt/lists/*

# ------------------------------------------------
# Delta Lake Jars
# IMPORTANT:
# Place jars directly inside /opt/spark/jars
# Spark auto-loads jars from this directory
# ------------------------------------------------

RUN curl -L -o /opt/spark/jars/delta-spark_2.12-3.1.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar

RUN curl -L -o /opt/spark/jars/delta-storage-3.1.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar

# ------------------------------------------------
# Hadoop AWS
# ------------------------------------------------

RUN curl -L -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

# ------------------------------------------------
# AWS SDK
# ------------------------------------------------

RUN curl -L -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# ------------------------------------------------
# Python Dependencies
# ------------------------------------------------

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# ------------------------------------------------
# AWS CLI
# ------------------------------------------------

RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" \
    -o awscliv2.zip && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf awscliv2.zip aws

# ------------------------------------------------
# Application
# ------------------------------------------------

WORKDIR /app

COPY . /app

# ------------------------------------------------
# Environment Variables
# ------------------------------------------------

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# ------------------------------------------------
# Default Command
# ------------------------------------------------

CMD ["/bin/bash"]