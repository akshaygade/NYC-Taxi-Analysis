FROM python:3.10

RUN pip install pandas sqlalchemy psycopg2 fastparquet 

WORKDIR /app

COPY ingest_data.py ingest_data.py

ENTRYPOINT [ "python" , "ingest_data.py" ]