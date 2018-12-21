FROM python:3.4

RUN pip install dynafed-storagestats

RUN mkdir -p /app/ugr/conf.d & \
    mkdir -p /app/log

ENTRYPOINT [ "dynafed-storage", "-v", "stats", "-c", "/app/ugr/conf.d", "--stdout" ]
CMD [ "--logfile","/app/log/dynafed_storagestats.log", "--loglevel", "WARNING" ]
