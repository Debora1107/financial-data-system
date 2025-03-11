   FROM python:3.9-slim

   WORKDIR /app

   COPY requirements.txt .

   RUN pip install --no-cache-dir -r requirements.txt

   COPY . .

   ENV PYTHONUNBUFFERED=1
   ENV PYTHONPATH=/app
   ENV FLASK_APP=start.py
   ENV FLASK_ENV=production

   EXPOSE 5000 5001 5002 5003 5004

   CMD ["python", "start.py", "--init-db", "--load-data"]