FROM swift:latest
USER root
RUN apt-get install libcurl4-openssl-dev
COPY . /root/
RUN cd /root && swift build && swift build -c release
EXPOSE 8338
EXPOSE 8339
CMD ["/root/.build/debug/Catena"]
