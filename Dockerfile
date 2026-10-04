FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY app/ .

RUN mkdir -p /app/data

# Command to run the application.
# -m (not a file path) so the live/ package resolves against /app as the root.
CMD ["python", "-m", "live.delta_main"]