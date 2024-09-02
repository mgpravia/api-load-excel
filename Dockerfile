FROM python:3.9-slim

RUN mkdir /app
WORKDIR /app

#COPY requirements.txt requirements.txt
#COPY app.py  app.py
#COPY utils.py  utils.py
COPY . .

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8089

CMD [ "python","app.py" ]