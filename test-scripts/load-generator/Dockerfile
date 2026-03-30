FROM python:3.10.19-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD [ "python", "-u", "-m", "load_generator.main" ]
