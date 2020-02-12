#!/bin/bash

IMAGE="omm_run_int"

echo "Get a proxy certificate"
cp $HOME/.ssl/cadcproxy.pem ./ || exit $?

echo "Run image ${IMAGE}"
docker run -m=7g --rm --name omm_run -v ${PWD}:/usr/src/app/ ${IMAGE} omm_run || exit $?

date
exit 0
