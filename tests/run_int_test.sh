#!/bin/bash

RUN_ROOT=/home/goliaths/work/cadc/int_test
OMM_ROOT=/home/goliaths/work/cadc/omm2caom2

# stop if a file has any content
file_is_zero() {
  if [[ -e  ${1} ]]
  then
    if [[ ! -s ${1} ]]
    then
      echo "${1} not generated."
      exit -1
    fi
  else
    echo "${1} should exist."
    exit -1
  fi
}

# stop if a file doesn't have content
file_is_not_zero() {
  if [[ -e  ${1} ]]
  then
    if [[ -s ${1} ]]
    then
      echo "${1} generated."
      exit -1
    fi
  else
    echo "${1} should exist."
    exit -1
  fi
}

# stop if a file exists
file_exists() {
  if [[ -e  ${1} ]]
  then
    echo "${1} should not exist."
    exit -1
  fi
}

# stop if a file has specific content
file_has_content() {
  if grep "${1}" "${2}"
  then
    echo "${1} not expected in ${2}."
    exit -1
  fi
}

# stop if a file does not have specific content
file_does_not_have_content() {
  if ! grep "${1}" "${2}"
  then
    echo "${1} expected in ${2}."
    exit -1
  fi
}

check_complete() {
  echo "check_${1}"
  failure_log="${RUN_ROOT}/${1}/logs/failure_log.txt"
  success_log="${RUN_ROOT}/${1}/logs/success_log.txt"
  xml="${RUN_ROOT}/${1}/${2}.fits.xml"
  prev="${RUN_ROOT}/${1}/${2}_prev.jpg"
  thumb="${RUN_ROOT}/${1}/${2}_prev_256.jpg"
  file_is_not_zero ${failure_log}
  file_is_zero ${success_log}
  file_is_zero ${xml}
  file_is_zero ${prev}
  file_is_zero ${thumb}
  # footprint generation is invoked
  file_does_not_have_content "caom2:bounds" ${xml}
}

check_failures() {
  echo 'check_failures'
  failure_log="${RUN_ROOT}/failures/logs/failure_log.txt"
  file_is_zero ${failure_log}
}

check_scrape() {
  echo 'check_scrape'
  failure_log="${RUN_ROOT}/scrape/logs/failure_log.txt"
  success_log="${RUN_ROOT}/scrape/logs/success_log.txt"
  xml="${RUN_ROOT}/scrape/C120902_sh2-132_J_old_SCIRED.fits.xml"
  file_is_not_zero ${failure_log}
  file_is_zero ${success_log}
  file_is_zero ${xml}
  # caom2repo service is not invoked
  file_has_content "caom2:metaChecksum" ${xml}
  # footprint generation is not invoked
  file_has_content "caom2:bounds" ${xml}
}

check_scrape_modify() {
  check_complete scrape_modify C170324_0054_SCI
}

check_store_ingest_modify() {
  check_complete store_ingest_modify C180616_0135_SCI
  # caom2repo service is working
  xml="${RUN_ROOT}/store_ingest_modify/C180616_0135_SCI.fits.xml"
  file_does_not_have_content "caom2:metaChecksum" ${xml}
}

check_ingest_modify_local() {
  check_complete ingest_modify_local C080121_0339_SCI
  # caom2repo service is working
  xml="${RUN_ROOT}/ingest_modify_local/C080121_0339_SCI.fits.xml"
  file_does_not_have_content "caom2:metaChecksum" ${xml}
}

check_ingest_modify() {
  echo 'check_ingest_modify'
  failure_log="${RUN_ROOT}/ingest_modify/logs/failure_log.txt"
  success_log="${RUN_ROOT}/ingest_modify/logs/success_log.txt"
  fname="C170323_domeflat_K_CALRED"
  xml="${RUN_ROOT}/ingest_modify/${fname}.fits.xml"
  prev="${RUN_ROOT}/ingest_modify/${fname}_prev.jpg"
  thumb="${RUN_ROOT}/ingest_modify/${fname}_prev_256.jpg"
  file_is_not_zero ${failure_log}
  file_is_zero ${success_log}
  file_exists ${xml}
  file_exists ${prev}
  file_exists ${thumb}
}


# copy the latest version of omm2caom2 code that's required for a python install
mkdir -p omm2caom2 || exit $?
cp ${OMM_ROOT}/setup.py omm2caom2 || exit $?
cp ${OMM_ROOT}/setup.cfg omm2caom2 || exit $?
cp ${OMM_ROOT}/README.md omm2caom2 || exit $?
mkdir -p ${OMM_ROOT}/omm2caom2 || exit $?
cp ${OMM_ROOT}/omm2caom2/*.py omm2caom2/omm2caom2 || exit $?

# build the container
docker build -f ./Dockerfile -t omm_run_int ./ || exit $?

# run the container permutations that I care about
for ii in failures scrape scrape_modify store_ingest_modify ingest_modify_local ingest_modify
do
  echo "Run ${ii} test case ..."
  run_dir=${RUN_ROOT}/${ii}
  if [[ -e ${run_dir}/logs/success_log.txt ]]
  then
    sudo rm ${run_dir}/logs/*.log || exit $?
    sudo rm ${run_dir}/logs/*.txt || exit $?
    if [[ ${ii} != "failures" && ${ii} != "ingest_modify" ]]
    then
      sudo rm ${run_dir}/*.xml || exit $?
    fi
    if [[ ${ii} != "failures" && ${ii} != "scrape" && ${ii} != "ingest_modify" ]]
    then
      sudo rm ${run_dir}/*.jpg || exit $?
    fi
  fi
  output="$(docker run --rm -v ${run_dir}:/usr/src/app omm_run_int omm_run 2>&1)"
  result=$?
  if [[ ${result} -ne 0 ]]
  then
    echo "omm_run failed for ${ii}"
    exit -1
  fi
  if [[ ${output} != *" correctly"* ]]
  then
    if [[ ${ii} != "failures" ]]
    then
      echo "${output}"
      echo "omm_run failed for ${ii}"
      exit -1
    fi
  fi
  check_${ii}

done

date
exit 0
