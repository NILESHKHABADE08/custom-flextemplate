FROM apache/beam_python3.10_sdk:latest

WORKDIR /app
COPY . /app

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python", "main.py"]
