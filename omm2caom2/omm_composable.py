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

from omm2caom2 import omm_preview_augmentation, omm_footprint_augmentation
from omm2caom2 import main_app_kwargs
from caom2 import obs_reader_writer


__all__ = ['Omm2Caom2Meta', 'Omm2Caom2Data', 'CadcException']


class CadcException(Exception):
    pass


# TODO configuration information from somewhere and somehow
RESOURCE_ID = 'ivo://cadc.nrc.ca/sc2repo'


class CaomExecute(object):
    """Abstract class that defines the operations common to all OMM
    Execute classes."""

    # TODO - a lot of this content should reside in the composable
    # library

    def __init__(self, obs_id, root_dir, collection, netrc):
        self.obs_id = obs_id
        self.root_dir = root_dir
        self.collection = collection
        self.working_dir = os.path.join(self.root_dir, self.obs_id)
        self.fname = '{}.fits'.format(obs_id)
        self.model_fqn = os.path.join(self.working_dir,
                                      '{}.xml'.format(self.fname))
        self.netrc = os.path.join(root_dir, netrc)
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)

    def _create_dir(self):
        """Create the working area if it does not already exist."""
        if not os.path.exists(self.working_dir):
            os.mkdir(self.working_dir)
            if not os.path.exists(self.working_dir):
                raise CadcException(
                    'Could not mkdir {}'.format(self.working_dir))

    def _cleanup(self):
        """Remove a directory and all its contents."""
        if os.path.exists(self.working_dir):
            for ii in os.listdir(self.working_dir):
                os.remove(os.path.join(self.working_dir, ii))
            os.rmdir(self.working_dir)

    def _check_credentials_exist(self):
        """Ensure named credentials exist in this environment."""
        if not os.path.exists(self.netrc):
            raise CadcException(
                'Credentials do not exist {}.'.format(self.netrc))

    def _repo_cmd_read(self):
        """Retrieve the existing observaton model metadata."""
        repo_cmd = 'caom2-repo read --resource-id {} --netrc {} ' \
                   '{} {} -o {}'.format(
                       RESOURCE_ID, self.netrc, self.collection,
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
            raise CadcException('Could not read observation in {}'.format(
                self.model_fqn))

    def _repo_cmd_delete(self):
        """Retrieve the existing observaton model metadata."""
        repo_cmd = 'caom2-repo delete --resource-id {} --netrc {} ' \
                   '{} {}'.format(
                    RESOURCE_ID, self.netrc, self.collection,
                    self.obs_id).split()
        try:
            output, outerr = subprocess.Popen(
                repo_cmd, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE).communicate()
            self.logger.debug(
                'Command {} had output {}'.format(repo_cmd, output))
            if len(outerr) > 0:
                raise CadcException(
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
                   '{} {}'.format(operation, RESOURCE_ID,
                                  self.netrc, self.model_fqn).split()
        try:
            output, outerr = subprocess.Popen(
                repo_cmd, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE).communicate()
            self.logger.debug(
                'Command {} had output {}'.format(repo_cmd, output))
            if len(outerr) > 0:
                raise CadcException(
                    '{} failed with {}'.format(repo_cmd, outerr))
            self.logger.debug(
                'Command {} had outerr {}'.format(repo_cmd, outerr))
        except Exception as e:
            self.logger.debug(
                'Error with command {}:: {}'.format(repo_cmd, e))
            raise CadcException('Could not store the observation in {}'.format(
                self.model_fqn))


class Omm2Caom2Meta(CaomExecute):
    """Defines the pipeline step for OMM ingestion of metadata into CAOM2.
    This requires access to only header information."""

    def __init__(self, obs_id, root_dir, collection, netrc):
        super(Omm2Caom2Meta, self).__init__(
            obs_id, root_dir, collection, netrc)

    def execute(self, context):
        self.logger.debug('Begin execute for {}'.format(__name__))
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
            'fname': self.fname,
            'out_obs_xml': self.model_fqn,
            'collection': self.collection,
            'netrc': self.netrc}}
        main_app_kwargs(**kwargs)

        self.logger.debug('store the xml')
        self._repo_cmd('create')

        self.logger.debug('clean up the workspace')
        self._cleanup()

        self.logger.debug('End execute for {}'.format(__name__))


class Omm2Caom2Data(CaomExecute):
    """Defines the pipeline step for OMM generation and ingestion of footprints
    and previews into CAOM2. These are all the operations that require
    access to the file on disk, not just the header data. """

    def __init__(self, obs_id, root_dir, collection, netrc):
        super(Omm2Caom2Data, self).__init__(
            obs_id, root_dir, collection, netrc)

    def execute(self, context):
        self.logger.debug('Begin execute for {}'.format(__name__))
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
        kwargs = {'working_directory': self.working_dir}
        omm_preview_augmentation.visit(observation, **kwargs)

    def _generate_footprint(self, observation):
        kwargs = {'working_directory': self.working_dir,
                  'science_file': '{}'.format(self.fname)}
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
                   '{} {} {} -o {}'.format(self.netrc, self.collection,
                                           self.obs_id, fqn).split()
        try:
            output, outerr = subprocess.Popen(
                data_cmd, stdout=subprocess.PIPE).communicate()
            self.logger.debug(
                'Command {} had output {}'.format(data_cmd, output))
            self.logger.debug(
                'Command {} had outerr {}'.format(data_cmd, outerr))
            if not os.path.exists(fqn):
                raise CadcException('Did not retrieve {}'.format(fqn))
        except Exception as e:
            self.logger.debug(
                'Error writing files {}:: {}'.format(self.model_fqn, e))
            raise CadcException('Could not store the observation in {}'.format(
                self.model_fqn))

