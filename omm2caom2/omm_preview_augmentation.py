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

from hashlib import md5

from caom2 import Artifact, ProductType, ReleaseType, ChecksumURI
from caom2 import Observation
from omm2caom2 import manage_composable, CaomName, OmmName

__all__ = ['visit']


def visit(observation, **kwargs):
    assert observation is not None, 'Input parameter must have a value.'
    assert isinstance(observation, Observation), \
        'Input parameter must be an Observation'

    working_dir = './'
    if 'working_directory' in kwargs:
        working_dir = kwargs['working_directory']
    if 'netrc_fqn' in kwargs:
        netrc_fqn = kwargs['netrc_fqn']
    else:
        raise manage_composable.CadcException(
            '\'netrc_fqn\' missing from kwargs.'.format())

    count = 0
    for i in observation.planes:
        plane = observation.planes[i]
        for j in plane.artifacts:
            artifact = plane.artifacts[j]
            if (artifact.uri.endswith('.fits.gz') or
                    artifact.uri.endswith('.fits')):
                file_id = CaomName(artifact.uri).get_file_id()
                file_name = OmmName(file_id).get_file_name()
                science_fqn = os.path.join(working_dir, file_name)
                if not os.path.exists(science_fqn):
                    raise manage_composable.CadcException(
                        '{} visit file not found'.format(science_fqn))
                logging.debug('working on file {}'.format(science_fqn))
                _do_prev(file_id, science_fqn, working_dir, netrc_fqn, plane)
                logging.debug(
                    'Completed preview generation for {}.'.format(file_id))
            count += 2
    return {'artifacts': count}


def _augment(plane, uri, fqn, product_type):
    plane.artifacts.add(_artifact_metadata(uri, fqn, product_type))


def _artifact_metadata(uri, fqn, product_type):
    md5uri = ChecksumURI(md5(open(fqn, 'rb').read()).hexdigest())
    return Artifact(uri, product_type, ReleaseType.DATA,
                    content_type='image/jpg',
                    content_length=os.path.getsize(fqn),
                    content_checksum=md5uri)


def _do_prev(file_id, science_fqn, working_dir, netrc_fqn, plane):
    preview_fqn = os.path.join(working_dir, OmmName(file_id).get_prev())
    thumb_fqn = os.path.join(working_dir, OmmName(file_id).get_thumb())

    if os.access(preview_fqn, 0):
        os.remove(preview_fqn)
    prev_cmd = 'fitscut --all --autoscale=99.5 --asinh-scale --jpg --invert ' \
               '--compass {}'.format(science_fqn)
    manage_composable.exec_cmd_redirect(prev_cmd, preview_fqn)

    if os.access(thumb_fqn, 0):
        os.remove(thumb_fqn)
    prev_cmd = 'fitscut --all --output-size=256 --autoscale=99.5 ' \
               '--asinh-scale --jpg --invert --compass {}'.format(science_fqn)
    manage_composable.exec_cmd_redirect(prev_cmd, thumb_fqn)

    _put_omm(preview_fqn, netrc_fqn)
    _put_omm(thumb_fqn, netrc_fqn)

    _augment(plane, OmmName(file_id).get_prev_uri(), preview_fqn,
             ProductType.PREVIEW)
    _augment(plane, OmmName(file_id).get_thumb_uri(), thumb_fqn,
             ProductType.THUMBNAIL)


def _put_omm(jpg_name, netrc_fqn):
    # cmd = ['cadc-data', 'put', '--cert', _MYCERT, 'OMM', preview_jpg256, '--archive-stream', 'raw']
    cmd = 'cadc-data put --netrc-file {} OMM {} --archive-stream raw'.format(
        netrc_fqn, jpg_name)
    manage_composable.exec_cmd(cmd)
