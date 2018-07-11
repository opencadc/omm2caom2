#!/bin/bash

MK_ROOT="/home/goliaths/work/cadc/int_test"
NETRC="/home/goliaths/work/cadc/test_netrc"

mk_config() {
  echo "working_directory: /usr/src/app
netrc_filename: /usr/src/app/test_netrc
resource_id: ivo://cadc.nrc.ca/sc2repo
todo_file_name: todo.txt
logging_level: INFO
log_to_file: True
log_file_directory: /usr/src/app/logs
failure_log_file_name: failure_log.txt
retry_file_name: retries.txt
stream: raw
" > ${1}
}

mk_docker_entrypoint_file() {
  echo "#!/bin/bash
\$\@
" > ${1}
  chmod 755 ${1} || exit $?
}

test_preconditions_failures() {
  echo 'test_preconditions_failures'
  echo "use_local_files: False
task_types:
  - ingest
" >> ${2}

  todo_file="${1}/todo.txt"
  echo "C050607_0540_SCI
c180101_0001_CAL
C180616_0135_SCI
" > ${todo_file}

}

test_preconditions_scrape() {
  echo 'test_preconditions_scrape'
  echo "use_local_files: True
task_types:
  - scrape
" >> ${2}

  cadc-data get --netrc ${NETRC} -o ${1}/C120902_sh2-132_J_old_SCIRED.fits.gz OMM C120902_sh2-132_J_old_SCIRED || exit $?
}

test_preconditions_scrape_modify() {
  echo 'test_preconditions_scrape_modify'
  echo "use_local_files: True
task_types:
  - scrape
  - modify
" >> ${2}

  cadc-data get -z --netrc ${NETRC} -o ${1}/C170324_0054_SCI.fits OMM C170324_0054_SCI || exit $?
}

test_preconditions_store_ingest_modify() {
  echo 'test_preconditions_store_ingest_modify'
  echo "use_local_files: True
task_types:
  - store
  - ingest
  - modify
" >> ${2}

  cadc-data get -z --netrc ${NETRC} -o ${1}/C180616_0135_SCI.fits OMM C180616_0135_SCI || exit $?
}

test_preconditions_ingest_modify_local() {
  echo 'test_preconditions_ingest_modify_local'
  echo "use_local_files: True
task_types:
  - ingest
  - modify
" >> ${2}

  cadc-data get -z --netrc ${NETRC} -o ${1}/C080121_0339_SCI.fits OMM C080121_0339_SCI || exit $?
  cadc-data get --netrc ${NETRC} -o ${1}/C180108_0002_SCI.fits.gz OMM C180108_0002_SCI || exit $?
}

test_preconditions_ingest_modify() {
  echo 'test_preconditions_ingest_modify'
  echo "use_local_files: False
task_types:
  - ingest
  - modify
" >> ${2}

  todo_file="${1}/todo.txt"
  echo "C170323_domeflat_K_CALRED
" > ${todo_file}
}

test_preconditions_todo_parameter() {
  echo 'test_preconditions_todo_parameter'
  echo "use_local_files: False
task_types:
  - ingest
" >> ${2}

  todo_file="${1}/abc.txt"
  echo "bc
" > ${todo_file}
}

cp ${NETRC} ${MK_ROOT}
cp /home/goliaths/work/cadc/Dockerfile ${MK_ROOT}
for ii in failures scrape scrape_modify store_ingest_modify ingest_modify_local ingest_modify todo_parameter
do
  cur_dir="${MK_ROOT}/${ii}"
  if [[ ! -e ${cur_dir} ]]
  then
      mkdir ${ii} || exit $?
  fi

  de_file="${cur_dir}/docker-entrypoint.sh"
  mk_docker_entrypoint_file ${de_file}

  config_file="${cur_dir}/config.yml"
  mk_config ${config_file}
  test_preconditions_${ii} ${cur_dir} ${config_file}

done
