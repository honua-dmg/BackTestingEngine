FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY app/ .

RUN mkdir -p /app/data

# Set permissions
RUN chmod +x *.py

# Command to run the application
CMD ["python", "Main.py"]