# Use the official Bitnami Spark image
FROM bitnami/spark:latest

# Install Python dependencies (PySpark and others if needed)
USER root
RUN pip3 install pyspark pandas

# Set working directory
WORKDIR /workspace

# Command to keep the container running
CMD tail -f /dev/null
