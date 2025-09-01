# Base Python image
# We use a slim version of Python 3.10 from mirror.gcr.io (a Google mirror for Docker Hub).
# This helps when Docker Hub has connectivity or rate-limit issues.
FROM mirror.gcr.io/library/python:3.10-slim

# Working directory inside the container
WORKDIR /app

# Copy only requirements.txt first to take advantage of build cache
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the project (except files ignored in .dockerignore)
COPY . .

# Default command to run the application
CMD ["python", "-u", "main.py"]