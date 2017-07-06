FROM swift:latest
USER root
RUN apt-get install libcurl4-openssl-dev
COPY . /root/
RUN cd /root && swift build && swift build -c release && ln -s /root/.build/release/Catena /usr/bin/catena
EXPOSE 8338
EXPOSE 8339
CMD ["/usr/bin/catena", "-m"]
