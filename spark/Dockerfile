FROM apache/spark:3.4.1-python3

USER root
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && chown -R 1001:0 /app
COPY . .

USER 1001
