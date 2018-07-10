# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2018.                            (c) 2018.
#  Government of Canada                 Gouvernement du Canada
#  National Research Council            Conseil national de recherches
#  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
#  All rights reserved                  Tous droits réservés
#
#  NRC disclaims any warranties,        Le CNRC dénie toute garantie
#  expressed, implied, or               énoncée, implicite ou légale,
#  statutory, of any kind with          de quelque nature que ce
#  respect to the software,             soit, concernant le logiciel,
#  including without limitation         y compris sans restriction
#  any warranty of merchantability      toute garantie de valeur
#  or fitness for a particular          marchande ou de pertinence
#  purpose. NRC shall not be            pour un usage particulier.
#  liable in any event for any          Le CNRC ne pourra en aucun cas
#  damages, whether direct or           être tenu responsable de tout
#  indirect, special or general,        dommage, direct ou indirect,
#  consequential or incidental,         particulier ou général,
#  arising from the use of the          accessoire ou fortuit, résultant
#  software.  Neither the name          de l'utilisation du logiciel. Ni
#  of the National Research             le nom du Conseil National de
#  Council of Canada nor the            Recherches du Canada ni les noms
#  names of its contributors may        de ses  participants ne peuvent
#  be used to endorse or promote        être utilisés pour approuver ou
#  products derived from this           promouvoir les produits dérivés
#  software without specific prior      de ce logiciel sans autorisation
#  written permission.                  préalable et particulière
#                                       par écrit.
#
#  This file is part of the             Ce fichier fait partie du projet
#  OpenCADC project.                    OpenCADC.
#
#  OpenCADC is free software:           OpenCADC est un logiciel libre ;
#  you can redistribute it and/or       vous pouvez le redistribuer ou le
#  modify it under the terms of         modifier suivant les termes de
#  the GNU Affero General Public        la “GNU Affero General Public
#  License as published by the          License” telle que publiée
#  Free Software Foundation,            par la Free Software Foundation
#  either version 3 of the              : soit la version 3 de cette
#  License, or (at your option)         licence, soit (à votre gré)
#  any later version.                   toute version ultérieure.
#
#  OpenCADC is distributed in the       OpenCADC est distribué
#  hope that it will be useful,         dans l’espoir qu’il vous
#  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
#  without even the implied             GARANTIE : sans même la garantie
#  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
#  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
#  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
#  General Public License for           Générale Publique GNU Affero
#  more details.                        pour plus de détails.
#
#  You should have received             Vous devriez avoir reçu une
#  a copy of the GNU Affero             copie de la Licence Générale
#  General Public License along         Publique GNU Affero avec
#  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
#  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
#                                       <http://www.gnu.org/licenses/>.
#
#  $Revision: 4 $
#
# ***********************************************************************
#

import logging
import os
import site
import traceback

from argparse import ArgumentParser
from datetime import datetime

from omm2caom2 import omm_preview_augmentation, omm_footprint_augmentation
from omm2caom2 import manage_composable, OmmName
from caom2 import obs_reader_writer


__all__ = ['Omm2Caom2Meta', 'Omm2Caom2Data', 'run_by_file',
           'Omm2Caom2LocalMeta', 'Omm2Caom2LocalData', 'Omm2Caom2Store',
           'Omm2Caom2Scrape', 'Omm2Caom2CompareChecksum', 'OrganizeExecutes',
           'CaomExecute']


class CaomExecute(object):
    """Abstract class that defines the operations common to all OMM
    Execute classes."""

    # TODO - a lot of this content should reside in the composable
    # library

    def __init__(self, config, task_type, obs_id):
        self.logger = logging.getLogger()
        self.logger.setLevel(config.logging_level)
        formatter = logging.Formatter(
            '%(asctime)s:%(levelname)s:%(name)-12s:%(lineno)d:%(message)s')
        for handler in self.logger.handlers:
            handler.setLevel(config.logging_level)
            handler.setFormatter(formatter)
        self.logging_level_param = self._set_logging_level_param(
            config.logging_level)
        omm_name = OmmName(obs_id)
        self.obs_id = omm_name.get_obs_id()
        self.root_dir = config.working_directory
        self.collection = config.collection
        self.working_dir = os.path.join(self.root_dir, self.obs_id)
        self.model_fqn = os.path.join(self.working_dir,
                                      omm_name.get_model_file_name())
        self.netrc_fqn = config.netrc_file
        self.resource_id = config.resource_id
        self.task_type = task_type

    def _create_dir(self):
        """Create the working area if it does not already exist."""
        if not os.path.exists(self.working_dir):
            os.mkdir(self.working_dir)
            if not os.path.exists(self.working_dir):
                raise manage_composable.CadcException(
                    'Could not mkdir {}'.format(self.working_dir))
            if not os.access(self.working_dir, os.W_OK | os.X_OK):
                raise manage_composable.CadcException(
                    '{} is not writeable.'.format(self.working_dir))

    def _cleanup(self):
        """Remove a directory and all its contents."""
        if os.path.exists(self.working_dir):
            for ii in os.listdir(self.working_dir):
                os.remove(os.path.join(self.working_dir, ii))
            os.rmdir(self.working_dir)

    def _check_credentials_exist(self):
        """Ensure named credentials exist in this environment."""
        if not os.path.exists(self.netrc_fqn):
            raise manage_composable.CadcException(
                'Credentials do not exist {}.'.format(self.netrc_fqn))

    def _repo_cmd_read(self):
        """Retrieve the existing observaton model metadata."""
        repo_cmd = 'caom2-repo read {} --resource-id {} --netrc {} ' \
                   '{} {} -o {}'.format(self.logging_level_param,
                                        self.resource_id, self.netrc_fqn,
                                        self.collection,
                                        self.obs_id, self.model_fqn)
        manage_composable.exec_cmd(repo_cmd)

    def _repo_cmd_delete(self):
        """Retrieve the existing observaton model metadata."""
        repo_cmd = 'caom2-repo delete {} --resource-id {} --netrc {} ' \
                   '{} {}'.format(self.logging_level_param,
                                  self.resource_id, self.netrc_fqn,
                                  self.collection,
                                  self.obs_id)
        try:
            manage_composable.exec_cmd(repo_cmd)
            if os.path.exists(self.model_fqn):
                os.remove(self.model_fqn)
        except manage_composable.CadcException as e:
            pass
        # TODO - how to tell the difference between 'it doesn't exist', and
        # there's a real failure to pay attention to?
        # raise CadcException('Could not delete the observation in {}'.format(
        #     self.model_fqn))

    def _repo_cmd(self, operation):
        """This repo operation will work for either create or update."""
        repo_cmd = 'caom2-repo {} {} --resource-id {} --netrc ' \
                   '{} {}'.format(operation, self.logging_level_param,
                                  self.resource_id, self.netrc_fqn,
                                  self.model_fqn)
        manage_composable.exec_cmd(repo_cmd)

    def _define_local_dirs(self):
        """when files are on disk don't worry about a separate directory
        per observation"""
        self.working_dir = self.root_dir
        self.model_fqn = os.path.join(
            self.working_dir, OmmName(self.obs_id).get_model_file_name())

    def _find_file_name_storage(self):
        ad_lookup = self._data_cmd_info()
        if ad_lookup is not None:
            self.fname = ad_lookup
        else:
            raise manage_composable.CadcException(
                'Could not find the file {} in storage as expected.'.format(
                    self.obs_id))

    def _data_cmd_info(self):
        cmd = 'cadc-data info {} --netrc-file {} OMM {}'.format(
            self.logging_level_param, self.netrc_fqn, self.obs_id)
        try:
            result = manage_composable.exec_cmd_info(cmd)
            looked_up_name = None
            if result is not None:
                looked_up_name = result.split('name: ')[1].split()[0]
                self.logger.debug(
                    'Found file name in storage {}'.format(looked_up_name))
            return looked_up_name
        except Exception as e:
            self.logger.debug(e)
            raise manage_composable.CadcException(
                'Failed to execute {} with {}.'.format(cmd, e))

    def _fits2caom2_cmd_local(self):
        fqn = os.path.join(self.working_dir, self.fname)
        uri = OmmName(self.obs_id).get_file_uri()
        plugin = self._find_fits2caom2_plugin()
        cmd = 'omm2caom2 {} --netrc {} --observation {} {} --out {} ' \
              '--plugin {} --local {} --lineage {}/{}'.format(
                self.logging_level_param, self.netrc_fqn, self.collection,
                self.obs_id, self.model_fqn, plugin, fqn, self.obs_id, uri)
        manage_composable.exec_cmd(cmd)

    def _compare_checksums(self):
        fqn = os.path.join(self.working_dir, self.fname)
        manage_composable.compare_checksum(
            self.netrc_fqn, self.collection, fqn)


    @staticmethod
    def _find_fits2caom2_plugin():
        packages = site.getsitepackages()
        return os.path.join(packages[0], 'omm2caom2/omm2caom2.py')

    @staticmethod
    def _set_logging_level_param(logging_level):
        lookup = {logging.DEBUG: '--debug',
                  logging.INFO: '--verbose',
                  logging.WARNING: '',
                  logging.ERROR: '--quiet'}
        if logging_level in lookup:
            result = lookup[logging_level]
        else:
            result = ''
        return result


class Omm2Caom2Meta(CaomExecute):
    """Defines the pipeline step for OMM ingestion of metadata into CAOM2.
    This requires access to only header information."""

    def __init__(self, config, obs_id):
        super(Omm2Caom2Meta, self).__init__(
            config, manage_composable.TaskType.INGEST, obs_id)

    def execute(self, context):
        self.logger.debug('Begin execute for {} Meta'.format(__name__))
        self.logger.debug('the steps:')
        self.logger.debug('make sure named credentials exist')
        self._check_credentials_exist()

        self.logger.debug('Find the file name as stored.')
        self._find_file_name_storage()

        self.logger.debug('create the work space, if it does not exist')
        self._create_dir()

        self.logger.debug('remove the existing observation, if it exists, '
                          'because metadata generation is less repeatable '
                          'for updates than for creates.')
        self._repo_cmd_delete()

        self.logger.debug('generate the xml, as the main_app will retrieve '
                          'the headers')
        self._fits2caom2_cmd()

        self.logger.debug('store the xml')
        self._repo_cmd('create')

        self.logger.debug('clean up the workspace')
        self._cleanup()

        self.logger.debug('End execute for {}'.format(__name__))

    def _fits2caom2_cmd(self):
        uri = OmmName(self.obs_id).get_file_uri()
        plugin = self._find_fits2caom2_plugin()
        cmd = 'omm2caom2 {} --netrc {} --observation {} {} --out {} ' \
              '--plugin {} --lineage {}/{}'.format(
                self.logging_level_param, self.netrc_fqn, self.collection,
                self.obs_id, self.model_fqn, plugin, self.obs_id, uri)
        manage_composable.exec_cmd(cmd)


class Omm2Caom2LocalMeta(CaomExecute):
    """Defines the pipeline step for OMM ingestion of metadata into CAOM2.
    The file containing the metadata is located on disk."""

    def __init__(self, config, obs_id, file_name):
        super(Omm2Caom2LocalMeta, self).__init__(
            config, manage_composable.TaskType.INGEST, obs_id)
        self._define_local_dirs()
        self.fname = file_name

    def execute(self, context):
        self.logger.debug('Begin execute for {} Meta'.format(__name__))
        self.logger.debug('the steps:')
        self.logger.debug('make sure named credentials exist')
        self._check_credentials_exist()

        self.logger.debug('remove the existing observation, if it exists, '
                          'because metadata generation is less repeatable '
                          'for updates than for creates.')
        self._repo_cmd_delete()

        self.logger.debug('generate the xml from the file on disk')
        self._fits2caom2_cmd_local()

        self.logger.debug('store the xml')
        self._repo_cmd('create')

        self.logger.debug('End execute for {}'.format(__name__))


class Omm2Caom2Data(CaomExecute):
    """Defines the pipeline step for OMM generation and ingestion of footprints
    and previews into CAOM2. These are all the operations that require
    access to the file on disk, not just the header data. """

    def __init__(self, config, obs_id):
        super(Omm2Caom2Data, self).__init__(
            config, manage_composable.TaskType.MODIFY, obs_id)
        self.log_file_directory = config.log_file_directory

    def execute(self, context):
        self.logger.debug('Begin execute for {} Data'.format(__name__))
        self.logger.debug('make sure named credentials exist')
        self._check_credentials_exist()

        self.logger.debug('Find the file name as stored.')
        self._find_file_name_storage()

        self.logger.debug('create the work space, if it does not exist')
        self._create_dir()

        self.logger.debug('get the input file')
        self._cadc_data_get()

        self.logger.debug('get the observation for the existing model')
        self._repo_cmd_read()
        observation = self._read_model()

        self.logger.debug('generate the previews')
        self._generate_previews(observation)

        self.logger.debug('generate the footprint')
        self._generate_footprint(observation)

        self.logger.debug('output the updated xml')
        self._write_model(observation)

        self.logger.debug('store the updated xml')
        self._repo_cmd('update')

        self.logger.debug('clean up the workspace')
        self._cleanup()

        self.logger.debug('End execute for {}'.format(__name__))

    def _generate_previews(self, observation):
        kwargs = {'working_directory': self.working_dir,
                  'netrc_fqn': self.netrc_fqn,
                  'logging_level_param': self.logging_level_param}
        omm_preview_augmentation.visit(observation, **kwargs)

    def _generate_footprint(self, observation):
        kwargs = {'working_directory': self.working_dir,
                  'science_file': self.fname,
                  'log_file_directory': self.log_file_directory}
        omm_footprint_augmentation.visit(observation, **kwargs)

    def _read_model(self):
        reader = obs_reader_writer.ObservationReader(False)
        observation = reader.read(self.model_fqn)
        return observation

    def _write_model(self, observation):
        writer = obs_reader_writer.ObservationWriter()
        writer.write(observation, self.model_fqn)

    def _cadc_data_get(self):
        """Retrieve a collection file, even if it already exists. This might
        ensure that the latest version of the file is retrieved from
        storage."""

        # all the gets are supposed to be unzipped, so that the
        # footprintfinder is more efficient, so make sure the fully
        # qualified output name isn't the gzip'd version

        if self.fname.endswith('.gz'):
            fqn = os.path.join(self.working_dir, self.fname.replace('.gz', ''))
        else:
            fqn = os.path.join(self.working_dir, self.fname)

        data_cmd = 'cadc-data get {} -z --netrc {} {} {} -o {}'.format(
            self.logging_level_param, self.netrc_fqn, self.collection,
            self.obs_id, fqn)
        manage_composable.exec_cmd(data_cmd)
        if not os.path.exists(fqn):
            raise manage_composable.CadcException(
                'Failed {}. Did not retrieve {}'.format(data_cmd, fqn))

    def _find_file_name(self):
        self._find_file_name_storage()


class Omm2Caom2LocalData(Omm2Caom2Data):
    """Defines the pipeline step for OMM generation and ingestion of footprints
    and previews into CAOM2. These are all the operations that require
    access to the file on disk. This class assumes it has access to the
    files on disk."""

    def __init__(self, config, obs_id, file_name):
        super(Omm2Caom2LocalData, self).__init__(config, obs_id)
        self._define_local_dirs()
        self.fname = file_name

    def execute(self, context):
        self.logger.debug('Begin execute for {} Data'.format(__name__))
        self.logger.debug('make sure named credentials exist')
        self._check_credentials_exist()

        self.logger.debug('get the observation for the existing model')
        self._repo_cmd_read()
        observation = self._read_model()

        self.logger.debug('generate the previews')
        self._generate_previews(observation)

        self.logger.debug('generate the footprint')
        self._generate_footprint(observation)

        self.logger.debug('output the updated xml')
        self._write_model(observation)

        self.logger.debug('store the updated xml')
        self._repo_cmd('update')

        self.logger.debug('End execute for {}'.format(__name__))


class Omm2Caom2Store(CaomExecute):
    """Defines the pipeline step for OMM storage of a file. This requires
    access to the file on disk. It will gzip compress the file."""

    def __init__(self, config, obs_id, file_name):
        super(Omm2Caom2Store, self).__init__(
            config, manage_composable.TaskType.STORE, obs_id)
        # when files are on disk don't worry about a separate directory
        # per observation
        self.working_dir = self.root_dir
        self.storage_host = config.storage_host
        self.stream = config.stream
        self.fname = file_name

    def execute(self, context):
        self.logger.debug('Begin execute for {} Data'.format(__name__))
        self.logger.debug('make sure named credentials exist')
        self._check_credentials_exist()

        self.logger.debug('store the input file to ad')
        self._cadc_data_put()

        self.logger.debug('End execute for {}'.format(__name__))

    def _cadc_data_put(self):
        """Store a collection file."""
        data_cmd = 'cadc-data put {} -c --netrc {} {} -s {} {}'.format(
            self.logging_level_param, self.netrc_fqn, self.collection,
            self.stream, self.fname)
        manage_composable.exec_cmd(data_cmd)


class Omm2Caom2Scrape(CaomExecute):
    """Defines the pipeline step for OMM creation of a CAOM2 model
    observation. The file containing the metadata is located on disk.
    No record is written to a web service."""

    def __init__(self, config, obs_id, file_name):
        super(Omm2Caom2Scrape, self).__init__(
            config, manage_composable.TaskType.SCRAPE, obs_id)
        self._define_local_dirs()
        self.fname = file_name

    def execute(self, context):
        self.logger.debug('Begin execute for {} Meta'.format(__name__))
        self.logger.debug('the steps:')

        self.logger.debug('generate the xml from the file on disk')
        self._fits2caom2_cmd_local()

        self.logger.debug('End execute for {}'.format(__name__))


class Omm2Caom2DataScrape(Omm2Caom2LocalData):
    """Defines the pipeline step for OMM generation and ingestion of footprints
    and previews with no update to the service at the end. These are all the
    operations that require access to the file on disk. This class assumes
    it has access to the files on disk. The organization of this class
    assumes the 'Scrape' task has been done previously, so the model
    instance exists on disk."""

    def __init__(self, config, obs_id, file_name):
        super(Omm2Caom2DataScrape, self).__init__(config, obs_id, file_name)

    def execute(self, context):
        self.logger.debug('Begin execute for {} Data'.format(__name__))

        self.logger.debug('get observation for the existing model from disk')
        observation = self._read_model()

        self.logger.debug('generate the previews')
        self._generate_previews(observation)

        self.logger.debug('generate the footprint')
        self._generate_footprint(observation)

        self.logger.debug('output the updated xml')
        self._write_model(observation)

        self.logger.debug('End execute for {}'.format(__name__))


class Omm2Caom2CompareChecksum(CaomExecute):
    """Defines the pipeline step for comparing the checksum of a file on disk
    with the checksum of the supposedly-the-same file stored at CADC.

    This step should be invoked with any other task type that relies on
    files on local disk.
    """

    def __init__(self, config, obs_id, file_name):
        super(Omm2Caom2CompareChecksum, self).__init__(
            config, manage_composable.TaskType.UNKNOWN, obs_id)
        self._define_local_dirs()
        self.fname = file_name

    def execute(self, context):
        self.logger.debug('Begin execute for {} '
                          'CompareChecksum'.format(__name__))
        self.logger.debug('the steps:')

        self.logger.debug('generate the xml from the file on disk')
        self._compare_checksums()

        self.logger.debug('End execute for {}'.format(__name__))


class OrganizeExecutes(object):
    """How to turn on/off various steps in the OMM pipeline."""

    def __init__(self, config, todo_file=None):
        self.config = config
        self.task_types = config.task_types
        self.logger = logging.getLogger()
        self.logger.setLevel(config.logging_level)
        if todo_file is not None:
            self.todo_fqn = todo_file
            todo_name = os.path.basename(todo_file).split('.')[0]
            self.success_fqn = os.path.join(self.config.log_file_directory,
                                            '{}_success.log'.format(todo_name))
            self.failure_fqn = os.path.join(self.config.log_file_directory,
                                            '{}_failure.log'.format(todo_name))
            self.retry_fqn = os.path.join(self.config.log_file_directory,
                                          '{}_retry.log'.format(todo_name))
        else:
            self.todo_fqn = config.work_fqn
            self.success_fqn = config.success_fqn
            self.failure_fqn = config.failure_fqn
            self.retry_fqn = config.retry_fqn

        if config.log_to_file:
            failure = open(self.failure_fqn, 'w')
            failure.close()
            retry = open(self.retry_fqn, 'w')
            retry.close()
            success = open(self.success_fqn, 'w')
            success.close()
        self.success_count = 0

    def choose(self, obs_id, file_name=None):
        executors = []
        if OmmName.is_valid(obs_id):
            for task_type in self.task_types:
                self.logger.debug(task_type)
                if task_type == manage_composable.TaskType.SCRAPE:
                    if self.config.use_local_files:
                        executors.append(
                            Omm2Caom2Scrape(self.config, obs_id, file_name))
                    else:
                        raise manage_composable.CadcException(
                            'use_local_files must be True with '
                            'Task Type "SCRAPE"')
                elif task_type == manage_composable.TaskType.STORE:
                    if self.config.use_local_files:
                        executors.append(
                            Omm2Caom2Store(self.config, obs_id, file_name))
                    else:
                        raise manage_composable.CadcException(
                            'use_local_files must be True with '
                            'Task Type "STORE"')
                elif task_type == manage_composable.TaskType.INGEST:
                    if self.config.use_local_files:
                        executors.append(
                            Omm2Caom2LocalMeta(self.config, obs_id, file_name))
                    else:
                        executors.append(Omm2Caom2Meta(self.config, obs_id))
                elif task_type == manage_composable.TaskType.MODIFY:
                    if self.config.use_local_files:
                        if isinstance(executors[0], Omm2Caom2Scrape):
                            executors.append(
                                Omm2Caom2DataScrape(self.config, obs_id,
                                                    file_name))
                        else:
                            executors.append(
                                Omm2Caom2LocalData(self.config, obs_id,
                                                   file_name))
                    else:
                        executors.append(Omm2Caom2Data(self.config, obs_id))
                else:
                    raise manage_composable.CadcException(
                        'Do not understand task type {}'.format(task_type))
            if (self.config.use_local_files and
                    manage_composable.TaskType.SCRAPE not in self.task_types):
                executors.append(
                    Omm2Caom2CompareChecksum(self.config, obs_id, file_name))
        else:
            logging.error('{} failed naming validation check. Must match '
                          'this regular expression {}.'.format(
                            obs_id, OmmName.OMM_NAME_PATTERN))
            self.capture_failure(obs_id, file_name, 'Invalid observation ID')
        return executors

    def capture_failure(self, obs_id, file_name, e):
        if self.config.log_to_file:
            failure = open(self.failure_fqn, 'a')
            try:
                min_error = self._minimize_error_message(e)
                failure.write(
                    '{} {} {} {}\n'.format(datetime.now(), obs_id, file_name,
                                           min_error))
            finally:
                failure.close()

            if not self.config.use_local_files:
                retry = open(self.retry_fqn, 'a')
                try:
                    retry.write('{}\n'.format(obs_id))
                finally:
                    retry.close()

    def capture_success(self, obs_id, file_name):
        self.success_count += 1
        if self.config.log_to_file:
            success = open(self.success_fqn, 'a')
            try:
                success.write(
                    '{} {} {}\n'.format(datetime.now(), obs_id, file_name))
            finally:
                success.close()

    @staticmethod
    def _minimize_error_message(e):
        """Turn the long-winded stack trace into something minimal that lends
        itself to awk."""
        if 'IllegalPolygonException' in e:
            return 'IllegalPolygonException'
        elif 'Read timed out' in e:
            return 'Read timed out'
        elif 'failed to load external entity' in e:
            return 'caom2repo xml error'
        elif 'Did not retrieve' in e:
            return 'cadc-data get error'
        elif 'NAXES was not set' in e:
            return 'NAXES was not set'
        elif 'Invalid SpatialWCS' in e:
            return 'Invalid SpatialWCS'
        elif 'getProxyCertificate failed' in e:
            return 'getProxyCertificate failed'
        else:
            return e


def _set_up_file_logging(config, obs_id):
    log_h = None
    if config.log_to_file:
        log_fqn = os.path.join(config.working_directory,
                               OmmName(obs_id).get_log_file())
        if config.log_file_directory is not None:
            if not os.path.exists(config.log_file_directory):
                os.mkdir(config.log_file_directory)
            log_fqn = os.path.join(config.log_file_directory,
                                   OmmName(obs_id).get_log_file())
        log_h = logging.FileHandler(log_fqn)
        formatter = logging.Formatter(
            '%(asctime)s:%(levelname)s:%(name)-12s:%(lineno)d:%(message)s')
        log_h.setLevel(config.logging_level)
        log_h.setFormatter(formatter)
        logging.getLogger().addHandler(log_h)
    return log_h


def _unset_file_logging(config, log_h):
    if config.log_to_file:
        logging.getLogger().removeHandler(log_h)


def _do_one(config, organizer, obs_id, file_name=None):
    log_h = _set_up_file_logging(config, obs_id)
    try:
        executors = organizer.choose(obs_id, file_name)
        for executor in executors:
            logging.info(
                'Step {} for {}'.format(executor.task_type, obs_id))
            executor.execute(context=None)
        if len(executors) > 0:
            organizer.capture_success(obs_id, file_name)
    except Exception as e:
        organizer.capture_failure(obs_id, file_name,
                                  e=traceback.format_exc())
        raise e
    finally:
        _unset_file_logging(config, log_h)


def _run_todo_file(config, organizer):
    with open(organizer.todo_fqn) as f:
        for line in f:
            obs_id = line.strip()
            logging.info('Process observation id {}'.format(obs_id))
            _do_one(config, organizer, obs_id, file_name=None)


def _run_local_files(config, organizer):
    todo_list = os.listdir(config.working_directory)
    for do_file in todo_list:
        if do_file.endswith('.fits') or do_file.endswith('.fits.gz'):
            logging.info('Process file {}'.format(do_file))
            obs_id = OmmName.remove_extensions(do_file)
            _do_one(config, organizer, obs_id, do_file)


def run_by_file():
    try:
        config = manage_composable.Config()
        config.get_executors()
        config.collection = 'OMM'
        logging.debug(config)
        logger = logging.getLogger()
        logger.setLevel(config.logging_level)
        if config.use_local_files:
            organize = OrganizeExecutes(config)
            _run_local_files(config, organize)
        else:
            parser = ArgumentParser()
            parser.add_argument('--todo',
                                help='Fully-qualified todo file name.')
            args = parser.parse_args()
            if args.todo is not None:
                organize = OrganizeExecutes(config, args.todo)
            else:
                organize = OrganizeExecutes(config)
            _run_todo_file(config, organize)
        logging.info('Processed {} correctly.'.format(organize.success_count))
    except Exception as e:
        logging.error(e)
        tb = traceback.format_exc()
        logging.error(tb)


def run_single(**argv):
    import sys
    config = manage_composable.Config()
    config.collection = 'OMM'
    config.working_directory = '/root/airflow'
    config.use_local_files = False
    config.logging_level = 'INFO'
    config.log_to_file = False
    config.task_types = [manage_composable.TaskType.INGEST]
    config.resource_id = 'ivo://cadc.nrc.ca/sc2repo'
    organizer = OrganizeExecutes(config)
    logging.error(sys.argv[2])
    logging.error(sys.argv[3])
    _do_one(config, organizer, sys.argv[1])
