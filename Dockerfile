FROM opencadc/matplotlib:3.9-slim

RUN apt-get update --no-install-recommends && \
    apt-get install -y build-essential \
                       git \
                       libjpeg-dev \
                       wget && \
    rm -rf /var/lib/apt/lists /tmp/* /var/tmp/*

RUN oldpath=`pwd` && cd /tmp && \
    wget http://www.eso.org/~fstoehr/footprintfinder.py && \
    cp footprintfinder.py /usr/local/lib/python3.9/site-packages/footprintfinder.py && \
    chmod 755 /usr/local/lib/python3.9/site-packages/footprintfinder.py && \
    cd $oldpath

RUN pip install cadcdata \
    cadctap \
    caom2 \
    caom2repo \
    caom2utils \
    importlib-metadata \
    python-dateutil \
    PyYAML \
    slackclient \
    spherical-geometry \
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

ARG CAOM2_BRANCH=master
ARG CAOM2_REPO=opencadc
ARG OPENCADC_BRANCH=master
ARG OPENCADC_REPO=opencadc
ARG PIPE_BRANCH=master
ARG PIPE_REPO=opencadc

RUN git clone https://github.com/opencadc/cadctools.git && \
    cd cadctools && \
    pip install ./cadcdata && \
    cd ..

RUN git clone https://github.com/${CAOM2_REPO}/caom2tools.git && \
    cd caom2tools && \
    git checkout ${CAOM2_BRANCH} && \
    pip install ./caom2utils && \
    cd ..

RUN pip install git+https://github.com/${OPENCADC_REPO}/caom2pipe@${OPENCADC_BRANCH}#egg=caom2pipe
  
RUN git clone https://github.com/${PIPE_REPO}/omm2caom2.git && \
  cd omm2caom2 && \
  git checkout ${PIPE_BRANCH} && \
  cd .. && \
  cp ./omm2caom2/omm2caom2/omm_docker_run_cleanup.py /usr/local/bin && \
  pip install ./omm2caom2 && \
  cp ./omm2caom2/docker-entrypoint.sh / && \
  cp ./omm2caom2/config.yml /

ENTRYPOINT ["/docker-entrypoint.sh"]
