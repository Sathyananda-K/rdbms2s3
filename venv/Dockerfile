FROM ubuntu
COPY driver/* /driver/
COPY src /app
COPY requirements.txt /app
WORKDIR /driver/
RUN apt-get update && apt-get upgrade -y \
&& apt-get install python3 python3-dev python3-pip nano unixodbc-dev libpq-dev curl -y \
&& curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
&& curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list \
&& DEBIAN_FRONTEND=noninteractive apt-get install libxml2 -y \
&& apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql17 \
&& dpkg -i /driver/ibm-iaccess-1.1.0.14-1.0.amd64.deb \
&& tar xvf v11.1.4fp6_linuxx64_odbc_cli.tar.gz \
&& cp /opt/ibm/iaccess/lib64/libcwb* /driver/ \
&& tar xvf unixODBC-2.3.9.tar.gz && cd unixODBC-2.3.9 && ./configure && make && make install && cd .. \
&& tar xvf psqlodbc-12.02.0000.tar.gz && cd psqlodbc-12.02.0000 && ./configure && make && make install && cd .. \
&& pip install -r /app/requirements.txt
WORKDIR /app
CMD [ "python3", "/app/db2s3.py" ]