FROM python:3.10

RUN pip install pandas

WORKDIR /app

COPY pipeline.py pipeline_2.py

ENTRYPOINT [ "python" , "pipeline_2.py" ]