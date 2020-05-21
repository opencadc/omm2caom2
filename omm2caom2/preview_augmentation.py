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

from caom2 import ProductType, ReleaseType
from caom2 import Observation
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
    if 'observable' in kwargs:
        observable = kwargs['observable']
    else:
        raise mc.CadcException('Need an observable parameter.')

    count = 0
    for plane in observation.planes.values():
        for artifact in plane.artifacts.values():
            if (artifact.uri.endswith('.fits.gz') or
                    artifact.uri.endswith('.fits')):
                file_id = mc.CaomName(artifact.uri).file_id
                file_name = mc.CaomName(artifact.uri).file_name
                science_fqn = os.path.join(working_dir, file_name)
                if not os.path.exists(science_fqn):
                    if science_fqn.endswith('.gz'):
                        science_fqn = science_fqn.replace('.gz', '')
                    else:
                        science_fqn = f'{science_fqn}.gz'
                    if not os.path.exists(science_fqn):
                        raise mc.CadcException(
                            f'{science_fqn} visit file not found')
                science_fqn = _unzip(science_fqn)
                logging.debug(f'working on file {science_fqn}')
                omm_name = OmmName(file_name=file_name)
                count += _do_prev(file_id, science_fqn, working_dir, plane,
                                  cadc_client, observable.metrics, omm_name)
    logging.info(f'Completed preview augmentation for '
                 f'{observation.observation_id}.')
    return {'artifacts': count}


def _augment(plane, uri, fqn, product_type):
    temp = None
    if uri in plane.artifacts:
        temp = plane.artifacts[uri]
    plane.artifacts[uri] = mc.get_artifact_metadata(fqn, product_type,
                                                    ReleaseType.DATA, uri,
                                                    temp)


def _do_prev(file_id, science_fqn, working_dir, plane, cadc_client, metrics,
             omm_name):
    preview = omm_name.prev
    preview_fqn = os.path.join(working_dir, preview)
    thumb = omm_name.thumb
    thumb_fqn = os.path.join(working_dir, thumb)

    if os.access(preview_fqn, 0):
        os.remove(preview_fqn)
    prev_cmd = f'fitscut --all --autoscale=99.5 --asinh-scale --jpg --invert ' \
               f'--compass {science_fqn}'
    mc.exec_cmd_redirect(prev_cmd, preview_fqn)

    if os.access(thumb_fqn, 0):
        os.remove(thumb_fqn)
    prev_cmd = f'fitscut --all --output-size=256 --autoscale=99.5 ' \
               f'--asinh-scale --jpg --invert --compass {science_fqn}'
    mc.exec_cmd_redirect(prev_cmd, thumb_fqn)

    prev_uri = omm_name.prev_uri
    thumb_uri = omm_name.thumb_uri
    _augment(plane, prev_uri, preview_fqn, ProductType.PREVIEW)
    _augment(plane, thumb_uri, thumb_fqn, ProductType.THUMBNAIL)
    if cadc_client is not None:
        _store_smalls(cadc_client, working_dir, preview, thumb, metrics)
    return 2


def _store_smalls(cadc_client, working_directory, preview_fname,
                  thumb_fname, metrics):
    mc.data_put(cadc_client, working_directory, preview_fname, COLLECTION,
                mime_type='image/jpeg', metrics=metrics)
    mc.data_put(cadc_client, working_directory, thumb_fname, COLLECTION,
                mime_type='image/jpeg', metrics=metrics)


def _unzip(science_fqn):
    if science_fqn.endswith('.gz'):
        logging.debug(f'Unzipping {science_fqn}.')
        unzipped_science_fqn = science_fqn.replace('.gz', '')
        import gzip
        with open(science_fqn, 'rb') as f_read:
            gz = gzip.GzipFile(fileobj=f_read)
            with open(unzipped_science_fqn, 'wb') as f_write:
                f_write.write(gz.read())
        return unzipped_science_fqn
    else:
        return science_fqn
