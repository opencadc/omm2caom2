FROM opencadc/matplotlib:3.6-alpine

# these layers are the common layers for footprintfinder construction

RUN apk --no-cache add \
        bash \
        git \
        g++ \
        libmagic \
        wget

RUN pip install cadcdata && \
        pip install cadctap && \
        pip install caom2repo && \
        pip install ftputil && \
        pip install PyYAML && \
        pip install spherical-geometry && \
        pip install vos

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
&& wget http://tdc-www.harvard.edu/software/wcstools/wcssubs-3.9.5.tar.gz \
&& tar zxvf wcssubs-3.9.5.tar.gz \
&& mv wcssubs-3.9.5 libwcs \
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
  cp ./omm2caom2/omm2caom2/omm_docker_run_cleanup.py /usr/local/bin && \
  pip install ./omm2caom2 && \
  cp ./omm2caom2/docker-entrypoint.sh /

RUN apk --no-cache del git \
    g++

ENTRYPOINT ["/docker-entrypoint.sh"]
