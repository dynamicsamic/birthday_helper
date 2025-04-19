FROM python:3.11-slim

WORKDIR /makobot

COPY requirements.txt .

RUN pip install --upgrade pip
RUN pip install -r requirements.txt --no-cache-dir

COPY app ./app

COPY log_config.conf main.py ./

RUN mkdir data
RUN mkdir excel_backup

CMD ["python", "main.py"]

