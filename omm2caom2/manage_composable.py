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
import yaml

from datetime import datetime

__all__ = ['CadcException', 'Config', 'get_datetime', 'to_float']


class CadcException(Exception):
    pass


class Config(object):
    """Configuration information that remains the same for all steps and all
     work in a pipeline execution."""

    def __init__(self):
        # the root directory for all executor operations
        self.working_directory = None

        # the file that contains the list of work to be passed through the
        # pipeline
        self.work_file = None
        # the fully qualified name for the work file
        self.work_fqn = None

        # credentials for any service calls
        self.netrc_file = None

        # which collection is addressed by the pipeline
        self.collection = None

        # changes expectations of the executors for handling files on disk
        self.use_local_files = False

        # which service instance to use
        self.resource_id = None

        # the logging level - enforced throughout the pipeline
        self.logging_level = None

        # the ad 'stream' that goes with the collection - use when storing
        # files
        self.stream = None

        # the ad 'host' to store files to - used for testing cadc-data put
        # commands only, should usually be None
        self.storage_host = None

    @property
    def working_directory(self):
        return self._working_directory

    @working_directory.setter
    def working_directory(self, value):
        self._working_directory = value

    @property
    def work_file(self):
        return self._work_file

    @work_file.setter
    def work_file(self, value):
        self._work_file = value
        if self.working_directory is not None:
            self.work_fqn = os.path.join(
                self.working_directory, self.work_file)

    @property
    def netrc_file(self):
        return self._netrc_file

    @netrc_file.setter
    def netrc_file(self, value):
        self._netrc_file = value

    @property
    def collection(self):
        return self._collection

    @collection.setter
    def collection(self, value):
        self._collection = value

    @property
    def use_local_files(self):
        return self._use_local_files

    @use_local_files.setter
    def use_local_files(self, value):
        self._use_local_files = value

    @property
    def resource_id(self):
        return self._resource_id

    @resource_id.setter
    def resource_id(self, value):
        self._resource_id = value

    @property
    def logging_level(self):
        return self._logging_level

    @logging_level.setter
    def logging_level(self, value):
        lookup = {'DEBUG': logging.DEBUG,
                  'INFO': logging.INFO,
                  'WARNING': logging.WARNING,
                  'ERROR': logging.ERROR}
        if value in lookup:
            self._logging_level = lookup[value]

    @property
    def stream(self):
        return self._stream

    @stream.setter
    def stream(self, value):
        self._stream = value

    @property
    def storage_host(self):
        return self._storage_host

    @storage_host.setter
    def storage_host(self, value):
        self._storage_host = value

    @staticmethod
    def _lookup(config, lookup, default):
        if lookup in config:
            result = config[lookup]
        else:
            result = default
        return result

    def __str__(self):
        return 'working_directory:: \'{}\' ' \
               'work_fqn:: \'{}\' ' \
               'netrc_file:: \'{}\' ' \
               'collection:: \'{}\' ' \
               'logging_level:: \'{}\''.format(
                self.working_directory, self.work_fqn, self.netrc_file,
                self.collection, self.logging_level)

    def get_executors(self):
        """Look up the configuration values in the data structure extracted
        from the configuration file."""
        try:
            config = self.get_config()
            self.working_directory = \
                self._lookup(config, 'working_directory', os.getcwd())
            self.work_file = self._lookup(config, 'todo_file_name', 'todo.txt')
            self.netrc_file = \
                self._lookup(config, 'netrc_filename', 'test_netrc')
            self.resource_id = self._lookup(
                config, 'resource_id', 'ivo://cadc.nrc.ca/sc2repo')
            self.use_local_files = bool(
                self._lookup(config, 'use_local_files', False))
            self.logging_level = self._lookup(config, 'logging_level', 'DEBUG')
            logging.error(self)
        except KeyError as e:
            raise CadcException(
                'Error in config file {}'.format(e))

    def get_config(self):
        """Return a configuration dictionary. Assumes a file named config.yml in
        the current working directory."""
        config_fqn = os.path.join(os.getcwd(), 'config.yml')
        config = self.load_config(config_fqn)
        if config is None:
            raise CadcException('Could not find the file {}'.format(config_fqn))
        return config

    @staticmethod
    def load_config(config_fqn):
        """Read a configuration as a YAML file.
        :param config_fqn the fully qualified name for the configuration file."""
        try:
            logging.debug('Begin load_config.')
            with open(config_fqn) as f:
                data_map = yaml.safe_load(f)
                logging.debug('End load_config.')
                return data_map
        except (yaml.scanner.ScannerError, FileNotFoundError) as e:
            logging.error(e)
            return None


def get_datetime(from_value):
    """
    Ensure datetime values are in MJD.
    :param from_value:
    :return: datetime instance
    """

    if from_value:
        try:
            return datetime.strptime(from_value, '%Y-%m-%dT%H:%M:%S')
        except ValueError:
            try:
                return datetime.strptime(from_value,
                                         '%Y-%m-%d %H:%M:%S.%f')
            except ValueError:
                try:
                    return datetime.strptime(from_value, '%Y-%m-%d')
                except ValueError:
                    logging.error(
                        'Cannot parse datetime {}'.format(from_value))
                    return None
    else:
        return None


def to_float(value):
    """Cast to float, without throwing an exception."""
    return float(value) if value is not None else None
