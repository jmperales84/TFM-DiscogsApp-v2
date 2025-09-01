FROM mirror.gcr.io/library/python:3.10-slim
WORKDIR /api
COPY app.py requirements.txt ./
RUN pip install -r requirements.txt
EXPOSE 8080
ENTRYPOINT ["uvicorn", "app:app", "--port", "8080", "--host", "0.0.0.0"]