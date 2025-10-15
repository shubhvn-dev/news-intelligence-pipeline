FROM apache/airflow:2.7.1-python3.11

# Install additional Python packages
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Switch to airflow user
USER airflow