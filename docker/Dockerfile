FROM apache/spark:3.5.2

USER root

# ------------------------------------------------
# System packages
# ------------------------------------------------
RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    groff \
    less \
    && rm -rf /var/lib/apt/lists/*

# ------------------------------------------------
# Upgrade pip
# ------------------------------------------------
RUN pip install --upgrade pip

# ------------------------------------------------
# AWS CLI
# ------------------------------------------------
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o awscliv2.zip && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf awscliv2.zip aws

# ------------------------------------------------
# Python dependencies (IMPORTANT FIX HERE)
# ------------------------------------------------
RUN pip install --no-cache-dir \
    pyspark==3.5.2 \
    delta-spark==3.1.0 \
    pyyaml \
    pytest \
    chispa \
    boto3

# ------------------------------------------------
# App setup
# ------------------------------------------------
WORKDIR /app
COPY . /app

ENV PYTHONPATH="/app"
ENV PYTHONUNBUFFERED=1

# ------------------------------------------------
# Hadoop AWS jars
# ------------------------------------------------
RUN curl -L -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

RUN curl -L -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# ------------------------------------------------
# ✅ DELTA CONFIG (FIXED - ENABLED)
# ------------------------------------------------
ENV PYSPARK_SUBMIT_ARGS="\
--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
pyspark-shell"

# ------------------------------------------------
# Default command (CI mode)
# ------------------------------------------------
CMD ["python3", "-m", "pytest", "tests/"]