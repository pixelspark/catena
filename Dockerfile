FROM swift:latest
USER root
RUN apt-get install libcurl4-openssl-dev
COPY . /root/
RUN cd /root && swift build
EXPOSE 8338
EXPOSE 8339
CMD ["/root/.build/debug/Catena"]
