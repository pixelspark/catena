FROM swift:latest
ENV CATENA_CONFIGURATION release

RUN adduser --system --group catena
RUN apt-get install libcurl4-openssl-dev
COPY . /root/
RUN cd /root && rm -rf .build
RUN cd /root && swift build -c $CATENA_CONFIGURATION
RUN chmod o+rwx /root/.build/$CATENA_CONFIGURATION/*
RUN mv /root/.build/$CATENA_CONFIGURATION/Catena /usr/bin/catena && mv /root/.build/$CATENA_CONFIGURATION/*.so /usr/lib/

RUN mkdir /data && chown catena:catena -R /data
# TODO: Init catena here (--init-only --seed=x)
VOLUME /data

EXPOSE 8338
EXPOSE 8339
USER catena
WORKDIR /data
ENTRYPOINT ["/usr/bin/catena"]
CMD ["-m","--no-local-discovery"]
#HEALTHCHECK --interval=5m --timeout=3s CMD curl -f http://localhost:8338/api || exit 1
