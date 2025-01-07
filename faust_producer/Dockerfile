FROM python:3.10-slim

# Set the working directory
WORKDIR /app

# Install system-level dependencies

# Copy application files
COPY requirements.txt /app
# Install Python dependencies
# RUN pip install --cache-dir=/pip_cache faust-streaming pandas pymongo scikit-learn
RUN pip install -r requirements.txt
# Expose the Faust web server port (default is 6066)
COPY faust_producer.py /app

# EXPOSE 6066

# # Add a healthcheck (optional)
# HEALTHCHECK --interval=30s --timeout=5s CMD curl --fail http://localhost:6066/ || exit 1

# Set the command to start the Faust worker
# CMD ["faust", "-A", "main", "worker", "-l", "info", "--web-port=6066"]
