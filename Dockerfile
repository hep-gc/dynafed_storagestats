FROM python:3.4

RUN pip install dynafed-storagestats

RUN mkdir -p /app/conf.d & \
    mkdir -p /app/log

ENTRYPOINT [ "dynafed-storage", "-v", "stats", "-c", "/app/conf.d", "--stdout" ]
CMD [ "--logfile","/app/log/dynafed_storagestats.log", "--loglevel", "WARNING" ]
