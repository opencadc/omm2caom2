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

from caom2 import Observation, CoordPolygon2D, ValueCoord2D, Chunk
from omm2caom2 import footprintfinder, manage_composable


__all__ = ['visit']


def visit(observation, **kwargs):
    assert observation is not None, 'Input parameter must have a value.'
    assert isinstance(observation, Observation), \
        'Input parameter must be an Observation'

    working_dir = './'
    if 'working_directory' in kwargs:
        working_dir = kwargs['working_directory']
    if 'science_file' in kwargs:
        science_file = kwargs['science_file']
    else:
        raise manage_composable.CadcException(
            'No science_file parameter provided to vistor '
            'for obs {}.'.format(observation.observation_id))
    # TODO - this moves location handling structures to other than the
    # main composable code - this could be MUCH better handled, just not
    # sure how right now
    log_file_directory = None
    if 'log_file_directory' in kwargs:
        log_file_directory = kwargs['log_file_directory']

    science_fqn = os.path.join(working_dir, science_file)
    if not os.path.exists(science_fqn):
        if science_fqn.endswith('.gz'):
            science_fqn = science_fqn.replace('.gz', '')
            if not os.path.exists(science_fqn):
                raise manage_composable.CadcException(
                    '{} visit file not found'.format(science_fqn))

    count = 0
    for i in observation.planes:
        plane = observation.planes[i]
        for j in plane.artifacts:
            artifact = plane.artifacts[j]
            for k in artifact.parts:
                part = artifact.parts[k]
                for chunk in part.chunks:
                    _update_position(chunk, science_fqn)
                    count += 1

    return_file = '{}_footprint.txt'.format(observation.observation_id)
    return_string_file = '{}_footprint_returnstring.txt'.format(
        observation.observation_id)
    _handle_footprint_logs(log_file_directory, return_file)
    _handle_footprint_logs(log_file_directory, return_string_file)
    return {'chunks': count}


def _update_position(chunk, science_fqn):
    """This function assumes that if the code got here, the science file is
    on disk."""
    logging.debug('Begin _update_position')
    assert isinstance(chunk, Chunk), 'Expecting type Chunk'

    if (chunk.position is not None
            and chunk.position.axis is not None):
        logging.debug('position exists, calculate footprints for {}.'.format(
            science_fqn))
        full_area, footprint_xc, footprint_yc, ra_bary, dec_bary, \
            footprintstring, stc = footprintfinder.main(
                '-r -f  {}'.format(science_fqn))
        logging.debug('footprintfinder result: full area {} '
                      'footprint xc {} footprint yc {} ra bary {} '
                      'dec_bary {} footprintstring {} stc {}'.format(
                        full_area, footprint_xc, footprint_yc, ra_bary,
                        dec_bary, footprintstring, stc))
        bounds = CoordPolygon2D()
        coords = None
        fp_results = stc.split('Polygon FK5')
        if len(fp_results) > 1:
            coords = fp_results[1].split()
        else:
            fp_results = stc.split('Polygon ICRS')
            if len(fp_results) > 1:
                coords = fp_results[1].split()

        if coords is None:
            raise manage_composable.CadcException(
                'Do not recognize footprint {}'.format(stc))

        index = 0
        while index < len(coords):
            vertex = ValueCoord2D(manage_composable.to_float(coords[index]),
                                  manage_composable.to_float(coords[index + 1]))
            bounds.vertices.append(vertex)
            index += 2
            logging.debug('Adding vertex\n{}'.format(vertex))
        chunk.position.axis.bounds = bounds
    logging.debug('Done _update_position.')


def _handle_footprint_logs(log_file_directory, log_file):
    if log_file_directory is not None:
        orig_log_fqn = os.path.join(os.getcwd(), log_file)
        if os.path.exists(orig_log_fqn):
            log_fqn = os.path.join(log_file_directory, log_file)
            os.rename(orig_log_fqn, log_fqn)
            logging.debug('Moving footprint log files from {} to {}'.format(
                orig_log_fqn, log_fqn))
