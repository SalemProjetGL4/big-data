FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    streamlit>=1.35.0 \
    plotly>=5.20.0 \
    pandas>=2.0.0 \
    wordcloud>=1.9.3 \
    matplotlib>=3.8.0 \
    psycopg2-binary>=2.9.9 \
    pymongo>=4.6.0

COPY . .

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
