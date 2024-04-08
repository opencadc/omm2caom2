ARG OPENCADC_PYTHON_VERSION=3.12
FROM opencadc/matplotlib:${OPENCADC_PYTHON_VERSION}-slim as builder
ARG OPENCADC_PYTHON_VERSION

RUN apt-get update --no-install-recommends && \
    apt-get install -y build-essential git && \
    rm -rf /var/lib/apt/lists /tmp/* /var/tmp/*

ADD http://www.eso.org/~fstoehr/footprintfinder.py /usr/local/lib/python${OPENCADC_PYTHON_VERSION}/site-packages/
RUN chmod 755 /usr/local/lib/python${OPENCADC_PYTHON_VERSION}/site-packages/footprintfinder.py

WORKDIR /usr/src/app

ARG OPENCADC_MASTER_BRANCH=master
ARG OPENCADC_BRANCH=main
ARG OPENCADC_REPO=opencadc

RUN git clone https://github.com/${OPENCADC_REPO}/caom2tools.git && \
    cd caom2tools && \
    git checkout ${OPENCADC_BRANCH} && \
    pip install ./caom2utils && \
    cd ..

RUN pip install git+https://github.com/${OPENCADC_REPO}/caom2pipe@${OPENCADC_BRANCH}#egg=caom2pipe

RUN git clone https://github.com/${OPENCADC_REPO}/omm2caom2.git && \
  cd omm2caom2 && \
  git checkout ${OPENCADC_MASTER_BRANCH} && \
  cd .. && \
  cp ./omm2caom2/omm2caom2/omm_docker_run_cleanup.py /usr/local/bin && \
  pip install ./omm2caom2 && \
  cp ./omm2caom2/config.yml /

FROM python:${OPENCADC_PYTHON_VERSION}-slim
WORKDIR /usr/src/app
ARG OPENCADC_PYTHON_VERSION

COPY --from=builder /usr/local/lib/python${OPENCADC_PYTHON_VERSION}/site-packages/ /usr/local/lib/python${OPENCADC_PYTHON_VERSION}/site-packages/
COPY --from=builder /usr/local/bin/* /usr/local/bin/
COPY --from=builder /config.yml /

# libmagic
COPY --from=builder /etc/magic /etc/magic
COPY --from=builder /etc/magic.mime /etc/magic.mime
COPY --from=builder /usr/lib/x86_64-linux-gnu/libmagic* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/file/magic.mgc /usr/lib/file/
COPY --from=builder /usr/share/misc/magic /usr/share/misc/magic
COPY --from=builder /usr/share/misc/magic.mgc /usr/share/misc/magic.mgc
COPY --from=builder /usr/share/file/magic.mgc /usr/share/file/magic.mgc

# fitsverify
COPY --from=builder /usr/lib/x86_64-linux-gnu/libcfitsio* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libcurl-gnutls* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libnghttp2* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/librtmp* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libssh2* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libpsl* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libldap* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/liblber* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libsasl* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libbrotli* /usr/lib/x86_64-linux-gnu/

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
