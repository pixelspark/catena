FROM swift:4.0
ARG CATENA_CONFIGURATION=release
ENV CATENA_CONFIGURATION $CATENA_CONFIGURATION

RUN adduser --system --group catena
RUN apt-get install libcurl4-openssl-dev
COPY . /root/
RUN cd /root && rm -rf .build
RUN cd /root && swift build -c $CATENA_CONFIGURATION
RUN chmod o+rwx /root/.build/$CATENA_CONFIGURATION/*
RUN mv /root/.build/$CATENA_CONFIGURATION/Catena /usr/bin/catena

RUN mkdir /data && chown catena:catena -R /data
VOLUME /data

EXPOSE 8338
EXPOSE 8339
USER catena
WORKDIR /data
ENTRYPOINT ["/usr/bin/catena"]
#HEALTHCHECK --interval=5m --timeout=3s CMD curl -f http://localhost:8338/api || exit 1
