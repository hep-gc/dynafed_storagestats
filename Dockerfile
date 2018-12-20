FROM python:3.4

COPY requirements.txt .

RUN pip install -r requirements.txt

RUN rm requirements.txt & \
    mkdir -p /etc/ugr/conf.d & \
    mkdir -p /opt/myapp

WORKDIR /opt/myapp

COPY dynafed_storagestats.py .

ENTRYPOINT [ "./dynafed_storagestats.py", "-d", "/etc/ugr/conf.d", "-v", "--memhost", "memcache" ]
CMD [ "--loglevel", "WARNING" ]
