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
import subprocess
import traceback

from omm2caom2 import omm_preview_augmentation, omm_footprint_augmentation
from omm2caom2 import manage_composable
from omm2caom2 import omm_augment, OmmName
from caom2 import obs_reader_writer


__all__ = ['Omm2Caom2Meta', 'Omm2Caom2Data', 'run_by_file',
           'Omm2Caom2LocalMeta', 'Omm2Caom2LocalData', 'Omm2Caom2Store',
           'Omm2Caom2Scrape', 'OrganizeExecutes']


class CaomExecute(object):
    """Abstract class that defines the operations common to all OMM
    Execute classes."""

    # TODO - a lot of this content should reside in the composable
    # library

    def __init__(self, config, obs_id):
        self.obs_id = OmmName(obs_id).get_obs_id()
        self.root_dir = config.working_directory
        self.collection = config.collection
        self.working_dir = os.path.join(self.root_dir, self.obs_id)
        # self.fname = '{}.fits'.format(obs_id)
        self.fname = OmmName(obs_id).get_file_name()
        self.model_fqn = os.path.join(self.working_dir,
                                      OmmName(obs_id).get_model_file_name())
        # '{}.xml'.format(self.fname))
        self.netrc_fqn = os.path.join(self.root_dir, config.netrc_file)
        self.resource_id = config.resource_id
        self.logger = logging.getLogger()
        self.logger.setLevel(config.logging_level)
        # logging.error('working dir is {}'.format(self.working_dir))

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
        repo_cmd = 'caom2-repo read --resource-id {} --netrc {} ' \
                   '{} {} -o {}'.format(
                       self.resource_id, self.netrc_fqn, self.collection,
                       self.obs_id, self.model_fqn).split()
        try:
            output, outerr = subprocess.Popen(
                repo_cmd, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE).communicate()
            self.logger.debug(
                'Command {} had output {}'.format(repo_cmd, output))
        except Exception as e:
            self.logger.debug(
                'Error with command {}:: {}'.format(repo_cmd, e))
            raise manage_composable.CadcException(
                'Could not read observation in {}'.format(
                self.model_fqn))

    def _repo_cmd_delete(self):
        """Retrieve the existing observaton model metadata."""
        repo_cmd = 'caom2-repo delete --resource-id {} --netrc {} ' \
                   '{} {}'.format(
                    self.resource_id, self.netrc_fqn, self.collection,
                    self.obs_id).split()
        try:
            output, outerr = subprocess.Popen(
                repo_cmd, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE).communicate()
            self.logger.debug(
                'Command {} had output {}'.format(repo_cmd, output))
            if outerr is not None and len(outerr) > 0:
                raise manage_composable.CadcException(
                    '{} failed with {}'.format(repo_cmd, outerr))
            os.remove(self.model_fqn)
        except Exception as e:
            self.logger.debug(
                'Error with command {}:: {}'.format(repo_cmd, e))
            # TODO - how to tell the difference between 'it doesn't exist', and
            # there's a real failure to pay attention to?
            # raise CadcException('Could not delete the observation in {}'.format(
            #     self.model_fqn))

    def _repo_cmd(self, operation):
        """This repo operation will work for either create or update."""
        repo_cmd = 'caom2-repo {} --resource-id {} --netrc ' \
                   '{} {}'.format(operation, self.resource_id,
                                  self.netrc_fqn, self.model_fqn).split()
        try:
            output, outerr = subprocess.Popen(
                repo_cmd, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE).communicate()
            self.logger.debug(
                'Command {} had output {}'.format(repo_cmd, output))
            if outerr is not None and len(outerr) > 0:
                raise manage_composable.CadcException(
                    '{} failed with {}'.format(repo_cmd, outerr))
        except Exception as e:
            self.logger.debug(
                'Error with command {}:: {}'.format(repo_cmd, e))
            raise manage_composable.CadcException(
                'Could not store the observation in {}'.format(
                self.model_fqn))


class Omm2Caom2Meta(CaomExecute):
    """Defines the pipeline step for OMM ingestion of metadata into CAOM2.
    This requires access to only header information."""

    def __init__(self, config, obs_id):
        super(Omm2Caom2Meta, self).__init__(config, obs_id)

    def execute(self, context):
        self.logger.debug('Begin execute for {} Meta'.format(__name__))
        self.logger.debug('the steps:')
        self.logger.debug('make sure named credentials exist')
        self._check_credentials_exist()

        self.logger.debug('create the work space, if it does not exist')
        self._create_dir()

        self.logger.debug('remove the existing observation, if it exists, '
                          'because metadata generation is less repeatable '
                          'for updates than for creates.')
        self._repo_cmd_delete()

        self.logger.debug('generate the xml, as the main_app will retrieve '
                          'the headers')
        kwargs = {'params': {
            'observation': self.fname.split('.')[0],
            'out_obs_xml': self.model_fqn,
            'collection': self.collection,
            'netrc': self.netrc_fqn,
            'debug': True}}
        omm_augment(**kwargs)

        self.logger.debug('store the xml')
        self._repo_cmd('create')

        self.logger.debug('clean up the workspace')
        self._cleanup()

        self.logger.debug('End execute for {}'.format(__name__))


class Omm2Caom2LocalMeta(CaomExecute):
    """Defines the pipeline step for OMM ingestion of metadata into CAOM2.
    The file containing the metadata is located on disk."""

    def __init__(self, config, obs_id):
        super(Omm2Caom2LocalMeta, self).__init__(config, obs_id)
        # when files are on disk don't worry about a separate directory
        # per observation
        self.working_dir = self.root_dir
        self.model_fqn = os.path.join(self.working_dir,
                                      OmmName(obs_id).get_model_file_name())

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
        fqn = os.path.join(self.working_dir, self.fname)
        kwargs = {'params': {
            'local': [fqn],
            'observation': self.fname.split('.')[0],
            'out_obs_xml': self.model_fqn,
            'collection': self.collection,
            'netrc': self.netrc_fqn,
            'logging_level': self.logger.getEffectiveLevel()}}
        omm_augment(**kwargs)

        self.logger.debug('store the xml')
        self._repo_cmd('create')

        self.logger.debug('End execute for {}'.format(__name__))


class Omm2Caom2Data(CaomExecute):
    """Defines the pipeline step for OMM generation and ingestion of footprints
    and previews into CAOM2. These are all the operations that require
    access to the file on disk, not just the header data. """

    def __init__(self, config, obs_id):
        super(Omm2Caom2Data, self).__init__(config, obs_id)

    def execute(self, context):
        self.logger.debug('Begin execute for {} Data'.format(__name__))
        self.logger.debug('make sure named credentials exist')
        self._check_credentials_exist()

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
                  'netrc_fqn': self.netrc_fqn}
        omm_preview_augmentation.visit(observation, **kwargs)

    def _generate_footprint(self, observation):
        kwargs = {'working_directory': self.working_dir,
                  'science_file': self.fname}
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
        fqn = os.path.join(self.working_dir, self.fname)
        data_cmd = 'cadc-data get -z --netrc ' \
                   '{} {} {} -o {}'.format(self.netrc_fqn, self.collection,
                                           self.obs_id, fqn).split()
        try:
            output, outerr = subprocess.Popen(
                data_cmd, stdout=subprocess.PIPE).communicate()
            self.logger.debug(
                'Command {} had output {}'.format(data_cmd, output))
            self.logger.debug(
                'Command {} had outerr {}'.format(data_cmd, outerr))
            if not os.path.exists(fqn):
                raise manage_composable.CadcException(
                    'Did not retrieve {}'.format(fqn))
        except Exception as e:
            self.logger.debug(
                'Error writing files {}:: {}'.format(self.model_fqn, e))
            raise manage_composable.CadcException(
                'Could not store the observation in {}'.format(
                    self.model_fqn))


class Omm2Caom2LocalData(CaomExecute):
    """Defines the pipeline step for OMM generation and ingestion of footprints
    and previews into CAOM2. These are all the operations that require
    access to the file on disk. This class assumes it has access to the
    files on disk."""

    def __init__(self, config, obs_id):
        super(Omm2Caom2LocalData, self).__init__(config, obs_id)
        # when files are on disk don't worry about a separate directory
        # per observation
        self.working_dir = self.root_dir
        self.model_fqn = os.path.join(self.working_dir,
                                      OmmName(obs_id).get_model_file_name())

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

    def _generate_previews(self, observation):
        kwargs = {'working_directory': self.working_dir,
                  'netrc_fqn': self.netrc_fqn}
        omm_preview_augmentation.visit(observation, **kwargs)

    def _generate_footprint(self, observation):
        kwargs = {'working_directory': self.working_dir,
                  'science_file': self.fname}
        omm_footprint_augmentation.visit(observation, **kwargs)

    def _read_model(self):
        reader = obs_reader_writer.ObservationReader(False)
        observation = reader.read(self.model_fqn)
        return observation

    def _write_model(self, observation):
        writer = obs_reader_writer.ObservationWriter()
        writer.write(observation, self.model_fqn)


class Omm2Caom2Store(CaomExecute):
    """Defines the pipeline step for OMM storage of a file. This requires
    access to the file on disk. It will gzip compress the file."""

    def __init__(self, config, obs_id):
        super(Omm2Caom2Store, self).__init__(config, obs_id)
        # when files are on disk don't worry about a separate directory
        # per observation
        self.working_dir = self.root_dir
        self.storage_host = config.storage_host
        self.stream = config.stream

    def execute(self, context):
        self.logger.debug('Begin execute for {} Data'.format(__name__))
        self.logger.debug('make sure named credentials exist')
        self._check_credentials_exist()

        self.logger.debug('store the input file to ad')
        self._cadc_data_put()

        self.logger.debug('End execute for {}'.format(__name__))

    def _cadc_data_put(self):
        """Store a collection file."""
        fqn = os.path.join(self.working_dir, self.fname)
        data_cmd = 'cadc-data put -c --netrc {} ' \
                   '{} {} {}'.format(self.netrc_fqn,
                                     self.collection, self.stream,
                                     fqn).split()
        try:
            output, outerr = subprocess.Popen(
                data_cmd, stdout=subprocess.PIPE).communicate()
            self.logger.debug(
                'Command {} had output {}'.format(data_cmd, output))
            self.logger.debug(
                'Command {} had outerr {}'.format(data_cmd, outerr))
        except Exception as e:
            self.logger.debug('Error writing file {}:: {}'.format(fqn, e))
            raise manage_composable.CadcException(
                'Could not store the file {}'.format(fqn))


class Omm2Caom2Scrape(CaomExecute):
    """Defines the pipeline step for OMM creation of a CAOM2 model
    observation. The file containing the metadata is located on disk.
    No record is written to a web service."""

    def __init__(self, config, obs_id):
        super(Omm2Caom2Scrape, self).__init__(config, obs_id)
        # when files are on disk don't worry about a separate directory
        # per observation
        self.working_dir = self.root_dir
        self.model_fqn = os.path.join(self.working_dir,
                                      OmmName(obs_id).get_model_file_name())

    def execute(self, context):
        self.logger.debug('Begin execute for {} Meta'.format(__name__))
        self.logger.debug('the steps:')

        self.logger.debug('generate the xml from the file on disk')
        fqn = os.path.join(self.working_dir, self.fname)
        kwargs = {'params': {
            'local': [fqn],
            'observation': self.fname.split('.')[0],
            'out_obs_xml': self.model_fqn,
            'collection': self.collection,
            'netrc': self.netrc_fqn,
            'logging_level': self.logger.getEffectiveLevel()}}
        omm_augment(**kwargs)

        self.logger.debug('End execute for {}'.format(__name__))


class Omm2Caom2DataScrape(CaomExecute):
    """Defines the pipeline step for OMM generation and ingestion of footprints
    and previews with no update to the service at the end. These are all the
    operations that require access to the file on disk. This class assumes
    it has access to the files on disk. The organization of this class
    assumes the 'Scrape' task has been done previously, so the model
    instance exists on disk."""

    def __init__(self, config, obs_id):
        super(Omm2Caom2DataScrape, self).__init__(config, obs_id)
        # when files are on disk don't worry about a separate directory
        # per observation
        self.working_dir = self.root_dir
        self.model_fqn = os.path.join(self.working_dir,
                                      OmmName(obs_id).get_model_file_name())

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

    def _generate_previews(self, observation):
        kwargs = {'working_directory': self.working_dir,
                  'netrc_fqn': self.netrc_fqn}
        omm_preview_augmentation.visit(observation, **kwargs)

    def _generate_footprint(self, observation):
        kwargs = {'working_directory': self.working_dir,
                  'science_file': self.fname}
        omm_footprint_augmentation.visit(observation, **kwargs)

    def _read_model(self):
        reader = obs_reader_writer.ObservationReader(False)
        observation = reader.read(self.model_fqn)
        return observation

    def _write_model(self, observation):
        writer = obs_reader_writer.ObservationWriter()
        writer.write(observation, self.model_fqn)


class OrganizeExecutes(object):
    """How to turn on/off various steps in the OMM pipeline."""

    def __init__(self, config):
        self.config = config
        self.task_types = config.task_types
        self.logger = logging.getLogger()
        self.logger.setLevel(config.logging_level)

    def choose(self, obs_id):
        execution = []
        for task_type in self.task_types:
            self.logger.error(task_type)
            if task_type == manage_composable.TaskType.SCRAPE:
                execution.append(Omm2Caom2Scrape(self.config, obs_id))
            elif task_type == manage_composable.TaskType.STORE:
                if self.config.use_local_files:
                    execution.append(Omm2Caom2Store(self.config, obs_id))
                else:
                    raise manage_composable.CadcException(
                        'use_local_files must be True with Task Type "STORE"')
            elif task_type == manage_composable.TaskType.INGEST:
                if self.config.use_local_files:
                    execution.append(Omm2Caom2LocalMeta(self.config, obs_id))
                else:
                    execution.append(Omm2Caom2Meta(self.config, obs_id))
            elif task_type == manage_composable.TaskType.ENHANCE:
                if self.config.use_local_files:
                    if isinstance(execution[0], Omm2Caom2Scrape):
                        execution.append(
                            Omm2Caom2DataScrape(self.config, obs_id))
                    else:
                        execution.append(
                            Omm2Caom2LocalData(self.config, obs_id))
                else:
                    execution.append(Omm2Caom2Data(self.config, obs_id))
            else:
                raise manage_composable.CadcException(
                    'Do not understand task type {}'.format(task_type))
        return execution


def _run_todo_file(config, organizer):
    with open(config.work_fqn) as f:
        for line in f:
            obs_id = line.strip()
            logging.info('Process {}'.format(obs_id))
            executors = organizer.choose(obs_id)
            for executor in executors:
                logging.info('Step {} for {}'.format(executor, obs_id))
                executor.execute(context=None)


def _run_local_files(config, organizer):
    todo_list = os.listdir(config.working_directory)
    for do_file in todo_list:
        if do_file.endswith('.fits') or do_file.endswith('.fits.gz'):
            logging.info('Process {}'.format(do_file))
            obs_id = do_file.split('.')[0]
            executors = organizer.choose(obs_id)
            for executor in executors:
                logging.info('Step {} for {}'.format(executor, obs_id))
                executor.execute(context=None)


def run_by_file():
    try:
        config = manage_composable.Config()
        config.get_executors()
        config.collection = 'OMM'
        logging.error(config)
        logger = logging.getLogger()
        logger.setLevel(config.logging_level)
        organize = OrganizeExecutes(config)
        if config.use_local_files:
            _run_local_files(config, organize)
        else:
            _run_todo_file(config, organize)
    except Exception as e:
        logging.error(e)
        tb = traceback.format_exc()
        logging.error(tb)
