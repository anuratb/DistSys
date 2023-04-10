FROM python:3-alpine

WORKDIR /usr/src/manage_brokers

COPY . .
RUN pip3 install --no-cache-dir -r requirements.txt

CMD ["python3","run.py"]