FROM apache/airflow:2.7.1

USER root

# Install wget, Firefox, and other dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    firefox-esr \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Download and install geckodriver
RUN wget -q https://github.com/mozilla/geckodriver/releases/download/v0.33.0/geckodriver-v0.33.0-linux64.tar.gz && \
    tar -xzf geckodriver-v0.33.0-linux64.tar.gz && \
    mv geckodriver /usr/local/bin/ && \
    chmod +x /usr/local/bin/geckodriver && \
    rm geckodriver-v0.33.0-linux64.tar.gz

# Switch back to airflow user
USER airflow

# Install Python packages (selenium)
RUN pip install --no-cache-dir selenium