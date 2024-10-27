FROM python:3.13
RUN apt-get update
COPY . /postgres_scripts
WORKDIR /postgres_scripts
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "db_setup.py"]