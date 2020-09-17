FROM opencadc/matplotlib:3.8-slim

# these layers are the common layers for footprintfinder construction
RUN apt-get update
RUN apt-get install -y build-essential \
    git \
    wget

RUN oldpath=`pwd` && cd /tmp && \
    wget http://www.eso.org/~fstoehr/footprintfinder.py && \
    cp footprintfinder.py /usr/local/lib/python3.8/site-packages/footprintfinder.py && \
    chmod 755 /usr/local/lib/python3.8/site-packages/footprintfinder.py && \
    cd $oldpath

RUN pip install cadcdata && \
    cadctap && \
    caom2 && \
    caom2repo && \
    caom2utils && \
    importlib-metadata && \
    ftputil && \
    pytz && \
    PyYAML && \
    slackclient && \
    spherical-geometry && \
    vos

RUN apt-get install -y libjpeg-dev

RUN git clone https://github.com/HEASARC/cfitsio && \
  cd cfitsio && \
  ./configure --prefix=/usr && \
  make -j 2 && \
  make shared && \
  make install && \
  make clean

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

ARG OPENCADC_BRANCH=master
ARG OPENCADC_REPO=opencadc
ARG OMC_REPO=opencadc-metadata-curation

RUN git clone https://github.com/${OPENCADC_REPO}/caom2tools.git --branch ${OPENCADC_BRANCH} --single-branch && \
    pip install ./caom2tools/caom2utils

RUN git clone https://github.com/${OMC_REPO}/caom2pipe.git && \
  pip install ./caom2pipe
  
RUN git clone https://github.com/${OMC_REPO}/omm2caom2.git && \
  cp ./omm2caom2/omm2caom2/omm_docker_run_cleanup.py /usr/local/bin && \
  pip install ./omm2caom2 && \
  cp ./omm2caom2/docker-entrypoint.sh / && \
  cp ./omm2caom2/config.yml /

ENTRYPOINT ["/docker-entrypoint.sh"]
