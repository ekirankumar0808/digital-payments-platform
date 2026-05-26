FROM apache/spark:3.5.2

USER root

# System deps
RUN apt-get update && apt-get install -y \
    curl unzip groff less && \
    rm -rf /var/lib/apt/lists/*

# Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# AWS CLI
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o awscliv2.zip && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf awscliv2.zip aws

WORKDIR /app
COPY . /app

ENV PYTHONPATH="/app"
ENV PYTHONUNBUFFERED=1

EXPOSE 8501

CMD ["python3", "-m", "pytest", "tests/"]