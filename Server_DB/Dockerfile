#Dockerfile
#-----------------------
# FROM mysql:8.0-debian
FROM postgres:latest

# COPY deploy.sh /docker-entrypoint-initdb.d/ 
# RUN chmod +x /always-initdb.d/deploy.sh
# here the flask app deploy script is copied
COPY . /bkr
WORKDIR /bkr

# RUN apt-get update
# RUN apt-get install -y python3
# RUN apt-get install -y python3-pip

# RUN pip install --upgrade pip --break-system-packages
# RUN pip install --trusted-host pypi.python.org flask --break-system-packages
# RUN pip install psycopg2-binary --break-system-packages
# RUN pip install mysql-connector-python --break-system-packages

# RUN pip install -r requirements.txt --break-system-packages

# ENV MYSQL_ROOT_PASSWORD="abc" 
ENV POSTGRES_USER="postgres"
ENV POSTGRES_PASSWORD="abc"
#host='localhost', user='root',password='abc'

# CMD ["mysqld"]
CMD ["postgres"]

EXPOSE 5000