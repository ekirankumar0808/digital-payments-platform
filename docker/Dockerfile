FROM apache/spark:3.5.2

USER root

# Install required Python packages
RUN pip install delta-spark==3.2.0 pyyaml pytest chispa

# Set working directory
WORKDIR /app

# Copy project files
COPY . /app

# Add project root to PYTHONPATH
ENV PYTHONPATH="${PYTHONPATH}:/app"

# Create python alias
RUN ln -s /usr/bin/python3 /usr/bin/python