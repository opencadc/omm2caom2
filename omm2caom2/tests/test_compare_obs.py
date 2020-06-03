# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2016.                            (c) 2016.
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
#
# ***********************************************************************
import logging
import os
import subprocess

from argparse import ArgumentParser, RawDescriptionHelpFormatter

from caom2 import get_differences, obs_reader_writer

RESOURCE_ID = 'ivo://cadc.nrc.ca/sc2repo'


def compare_obs():
    logging.getLogger().setLevel(logging.INFO)
    parser = ArgumentParser(add_help=False,
                            formatter_class=RawDescriptionHelpFormatter)
    parser.description = ('Call get_differences for an observation, '
                          'one from Operations, and one from the sandbox. '
                          'Assumes there is a $HOME/.netrc file for '
                          'access control. Cleans up after itself. '
                          'Expected is the operational version, actual '
                          'is the sandbox version.')
    parser.add_argument('--collection', help='For example, OMM')
    parser.add_argument('--observationID', help=('The one that might contain '
                                                 'words, not the UUID.'))
    args = parser.parse_args()
    logging.info(args)
    cwd = os.getcwd()
    sb_model_fqn = os.path.join(cwd, f'{args.observationID}.sb.xml')
    ops_model_fqn = os.path.join(cwd, f'{args.observationID}.ops.xml')
    sb_obs = _get_obs(args.collection, args.observationID, sb_model_fqn,
                      RESOURCE_ID)
    ops_obs = _get_obs(args.collection, args.observationID, ops_model_fqn)
    result = get_differences(ops_obs, sb_obs, parent=None)
    if result is not None:
        logging.error('::')
        logging.error('::')
        logging.error('Expected is from the operational version, actual is '
                      'the sandbox version.')
        logging.error('\n'.join(x for x in result))
    else:
        logging.info('The observations appear to be the same')


def _get_obs(collection, obs_id, model_fqn, rid=None):
    _read_to_file(collection, obs_id, model_fqn, rid)
    return _read_obs(model_fqn)


def _read_to_file(collection, obs_id, model_fqn, rid):
    """Retrieve the existing observaton model metadata."""
    if rid is not None:
        repo_cmd = f'caom2-repo read --resource-id {rid} -n ' \
                   f'{collection} {obs_id} -o {model_fqn}'.split()
    else:
        repo_cmd = f'caom2-repo read -n {collection} {obs_id} -o ' \
                   f'{model_fqn}'.split()

    try:
        output, outerr = subprocess.Popen(
            repo_cmd, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE).communicate()
        logging.info(f'Command {repo_cmd} had output {output}')
    except Exception as e:
        logging.error(f'Error with command {repo_cmd}:: {e}')
        raise RuntimeError('broken')


def _read_obs(model_fqn):
    reader = obs_reader_writer.ObservationReader(False)
    result = reader.read(model_fqn)
    return result


if __name__ == "__main__":
    compare_obs()
