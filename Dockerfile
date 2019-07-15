FROM python:3.6-alpine
  
RUN apk --no-cache add \
        bash \
        coreutils \
        gcc \
        git \
        g++ \
        libffi-dev \
        libmagic \
        libxml2-dev \
        libxslt-dev \
        make \
        musl-dev \
        openssl-dev

RUN apk --no-cache add \
    freetype-dev \
    libpng-dev \
    gfortran \
    openblas-dev \
    py-numpy \
    py-pip \
    python \
    python-dev \
    wget

RUN pip install matplotlib

RUN oldpath=`pwd` && cd /tmp && \
    wget http://www.eso.org/~fstoehr/footprintfinder.py && \
    cp footprintfinder.py /usr/local/lib/python3.6/site-packages/footprintfinder.py && \
    chmod 755 /usr/local/lib/python3.6/site-packages/footprintfinder.py && \
    cd $oldpath

RUN git clone https://github.com/HEASARC/cfitsio && \
  cd cfitsio && \
  ./configure --prefix=/usr && \
  make -j 2 && \
  make shared && \
  make install && \
  make clean

RUN apk --no-cache add libjpeg-turbo-dev

RUN oldpath=`pwd` && cd /tmp \
&& git clone https://github.com/spacetelescope/fitscut \
&& cd fitscut \
&& wget ftp://cfa-ftp.harvard.edu/pub/gsc/WCSTools/wcssubs-3.9.0.tar.gz \
&& tar zxvf wcssubs-3.9.0.tar.gz \
&& mv wcssubs-3.9.0 libwcs \
&& cd libwcs \
&& make \
&& cp libwcs.a /usr/lib \
&& cd .. \
&& ./configure --prefix=/usr \
&& make \
&& make install \
&& make clean \
&& cd $oldpath \
&& rm -Rf /tmp/fitscut*

WORKDIR /usr/src/app
RUN git clone https://github.com/opencadc-metadata-curation/caom2tools.git && \
  cd caom2tools && git pull origin master && \
  pip install ./caom2utils && pip install ./caom2pipe && cd ..
  
RUN git clone https://github.com/opencadc-metadata-curation/omm2caom2.git && \
  cp /usr/local/bin/footprintfinder.py ./omm2caom2/omm2caom2 && \
  cp ./omm2caom2/omm2caom2/omm_docker_run_cleanup.py /usr/local/bin && \
  pip install ./omm2caom2

COPY ./docker-entrypoint.sh ./

ENTRYPOINT ["./docker-entrypoint.sh"]

