FROM opencadc/matplotlib

# these layers are the common layers for footprintfinder construction

RUN apk --no-cache add \
        bash \
        coreutils \
        git \
        libmagic \
        make
        
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

RUN oldpath=`pwd` && cd /tmp && \
    wget http://www.eso.org/~fstoehr/footprintfinder.py && \
    cp footprintfinder.py /usr/local/lib/python3.7/site-packages/footprintfinder.py && \
    chmod 755 /usr/local/lib/python3.7/site-packages/footprintfinder.py && \
    cd $oldpath

RUN pip install cadcdata && \
        pip install cadctap && \
        pip install caom2 && \
        pip install caom2repo && \
        pip install caom2utils && \
        pip install PyYAML && \
        pip install spherical-geometry && \
        pip install vos

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
RUN git clone https://github.com/opencadc-metadata-curation/caom2pipe.git && \
  pip install ./caom2pipe
  
RUN git clone https://github.com/opencadc-metadata-curation/omm2caom2.git && \
  cp ./omm2caom2/omm2caom2/omm_docker_run_cleanup.py /usr/local/bin && \
  pip install ./omm2caom2

COPY ./docker-entrypoint.sh ./

ENTRYPOINT ["./docker-entrypoint.sh"]

