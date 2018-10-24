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

from caom2 import Artifact, ProductType, ReleaseType, ChecksumURI
from caom2 import Observation
from caom2pipe import execute_composable as ec
from caom2pipe import manage_composable as mc
from omm2caom2 import OmmName, COLLECTION

__all__ = ['visit']


def visit(observation, **kwargs):
    mc.check_param(observation, Observation)

    working_dir = './'
    if 'working_directory' in kwargs:
        working_dir = kwargs['working_directory']
    if 'cadc_client' in kwargs:
        cadc_client = kwargs['cadc_client']
    else:
        raise mc.CadcException('Need a cadc_client parameter.')

    count = 0
    for i in observation.planes:
        plane = observation.planes[i]
        for j in plane.artifacts:
            artifact = plane.artifacts[j]
            if (artifact.uri.endswith('.fits.gz') or
                    artifact.uri.endswith('.fits')):
                file_id = ec.CaomName(artifact.uri).file_id
                file_name = ec.CaomName(artifact.uri).file_name
                science_fqn = os.path.join(working_dir, file_name)
                if not os.path.exists(science_fqn):
                    file_name = \
                        ec.CaomName(artifact.uri).uncomp_file_name
                    science_fqn = os.path.join(working_dir, file_name)
                    if not os.path.exists(science_fqn):
                        raise mc.CadcException(
                            '{} preview visit file not found'.format(
                                science_fqn))
                logging.debug('working on file {}'.format(science_fqn))
                count += _do_prev(file_id, science_fqn, working_dir, plane, cadc_client)
    logging.info('Completed preview augmentation for {}.'.format(
            observation.observation_id))
    return {'artifacts': count}


def _augment(plane, uri, fqn, product_type):
    temp = None
    if uri in plane.artifacts:
        temp = plane.artifacts[uri]
    plane.artifacts[uri] = _artifact_metadata(uri, fqn, product_type, temp)


def _artifact_metadata(uri, fqn, product_type, artifact):
    local_meta = mc.get_file_meta(fqn)
    md5uri = ChecksumURI('md5:{}'.format(local_meta['md5sum']))
    if artifact is None:
        return Artifact(uri, product_type, ReleaseType.DATA, local_meta['type'],
                        local_meta['size'], md5uri)
    else:
        artifact.product_type = product_type
        artifact.content_type = local_meta['type']
        artifact.content_length = local_meta['size']
        artifact.content_checksum = md5uri
        return artifact

def _do_prev(file_id, science_fqn, working_dir, plane, cadc_client):
    preview = OmmName(file_id).prev
    preview_fqn = os.path.join(working_dir, preview)
    thumb = OmmName(file_id).thumb
    thumb_fqn = os.path.join(working_dir, thumb)

    if os.access(preview_fqn, 0):
        os.remove(preview_fqn)
    prev_cmd = 'fitscut --all --autoscale=99.5 --asinh-scale --jpg --invert ' \
               '--compass {}'.format(science_fqn)
    mc.exec_cmd_redirect(prev_cmd, preview_fqn)

    if os.access(thumb_fqn, 0):
        os.remove(thumb_fqn)
    prev_cmd = 'fitscut --all --output-size=256 --autoscale=99.5 ' \
               '--asinh-scale --jpg --invert --compass {}'.format(science_fqn)
    mc.exec_cmd_redirect(prev_cmd, thumb_fqn)

    prev_uri = OmmName(file_id).prev_uri
    thumb_uri = OmmName(file_id).thumb_uri
    _augment(plane, prev_uri, preview_fqn, ProductType.PREVIEW)
    _augment(plane, thumb_uri, thumb_fqn, ProductType.THUMBNAIL)
    if cadc_client is not None:
        _store_smalls(cadc_client, working_dir, preview, thumb)
    return 2


def _store_smalls(cadc_client, working_directory, preview_fname,
                  thumb_fname):
    cwd = os.getcwd()
    try:
        os.chdir(working_directory)
        cadc_client.put_file(COLLECTION, preview_fname, 'raw')
        cadc_client.put_file(COLLECTION, thumb_fname, 'raw')
    except Exception as e:
        raise mc.CadcException('Failed to store previews with {}'.format(e))
    finally:
        os.chdir(cwd)
    mc.compare_checksum_client(cadc_client, COLLECTION,
                               os.path.join(working_directory, preview_fname))
    mc.compare_checksum_client(cadc_client, COLLECTION,
                               os.path.join(working_directory, thumb_fname))
