# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2025.                            (c) 2025.
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

import os
import pytest

from unittest.mock import patch

from cadcdata import FileInfo
from caom2 import ChecksumURI
from omm2caom2 import footprint_augmentation, preview_augmentation
from omm2caom2 import OmmName, cleanup_augmentation
from caom2pipe.manage_composable import CadcException, Observable, read_obs_from_file


TEST_OBS = 'C170324_0054'
TEST_FILE = f'{TEST_OBS}_SCI.fits.gz'
TEST_FILES_DIR = '/test_files'


def test_footprint_aug_visit():
    with pytest.raises(AssertionError):
        footprint_augmentation.visit(None)


def test_footprint_update_position(test_config, test_data_dir):
    fqn = f'{TEST_FILES_DIR}/{TEST_FILE}'
    omm_name = OmmName(source_names=[fqn])
    test_kwargs = {'storage_name': omm_name}
    test_fqn = os.path.join(test_data_dir, f'{omm_name.product_id}.expected.xml')
    test_obs = read_obs_from_file(test_fqn)
    test_chunk = (
        test_obs.planes[omm_name.product_id]
        .artifacts[omm_name.file_uri]
        .parts['0']
        .chunks[0]
    )
    assert test_chunk.position.axis.bounds is None

    # expected failure due to required kwargs parameter
    with pytest.raises(CadcException):
        test_result = footprint_augmentation.visit(test_obs)

    test_kwargs['working_directory'] = TEST_FILES_DIR
    test_result = footprint_augmentation.visit(test_obs, **test_kwargs)
    assert test_result is not None, 'expected a visit return value'
    assert (
        test_chunk.position.axis.bounds is not None
    ), 'bound calculation failed'


def test_preview_aug_visit():
    with pytest.raises(CadcException):
        preview_augmentation.visit(None)


def test_preview_augment_plane(test_config, test_data_dir):
    fqn = f'{TEST_FILES_DIR}/{TEST_FILE}'
    omm_name = OmmName(source_names=[fqn])
    preview = os.path.join(TEST_FILES_DIR, omm_name.prev)
    thumb = os.path.join(TEST_FILES_DIR, omm_name.thumb)
    if os.path.exists(preview):
        os.remove(preview)
    if os.path.exists(thumb):
        os.remove(thumb)
    test_fqn = os.path.join(test_data_dir, f'{omm_name.product_id}.expected.xml')
    test_obs = read_obs_from_file(test_fqn)
    assert len(test_obs.planes[omm_name.product_id].artifacts) == 1
    preva = f'{test_config.scheme}:{test_config.collection}/C170324_0054_SCI_prev.jpg'
    thumba = f'{test_config.scheme}:{test_config.collection}/C170324_0054_SCI_prev_256.jpg'

    test_config.observe_execution = True
    test_config.rejected_directory = test_data_dir
    test_config.rejected_file_name = 'rejected.yml'
    if os.path.exists(test_config.rejected_fqn):
        os.unlink(test_config.rejected_fqn)
    test_observable = Observable(test_config)

    test_kwargs = {
        'working_directory': TEST_FILES_DIR,
        'clients': None,
        'observable': test_observable,
        'storage_name': omm_name,
    }
    test_result = preview_augmentation.visit(test_obs, **test_kwargs)
    assert test_result is not None, 'expected a visit return value'
    assert os.path.exists(preview)
    assert os.path.exists(thumb)
    test_plane = test_result.planes[omm_name.product_id]
    assert test_plane.artifacts[preva].content_checksum == ChecksumURI(
        'md5:6fc37f04e7d16adf40f6279db9c5c8e1'
    ), 'prev checksum failure'
    assert test_plane.artifacts[thumba].content_checksum == ChecksumURI(
        'md5:f755cf9a75f1fb08ee020f3cd6c9b930'
    ), 'thumb checksum failure'

    # now do updates
    test_obs.planes[omm_name.product_id].artifacts[
        preva
    ].content_checksum = ChecksumURI('md5:de9f39804f172682ea9b001f8ca11f15')
    test_obs.planes[omm_name.product_id].artifacts[
        thumba
    ].content_checksum = ChecksumURI('m5:cd118dae04391f6bea93ba4bf2711adf')
    test_result = preview_augmentation.visit(test_obs, **test_kwargs)
    assert test_result is not None, 'expected update visit return value'
    assert len(test_result.planes[omm_name.product_id].artifacts) == 3
    assert os.path.exists(preview)
    assert os.path.exists(thumb)
    assert test_plane.artifacts[preva].content_checksum == ChecksumURI(
        'md5:6fc37f04e7d16adf40f6279db9c5c8e1'
    ), 'prev update failed'
    assert test_plane.artifacts[thumba].content_checksum == ChecksumURI(
        'md5:f755cf9a75f1fb08ee020f3cd6c9b930'
    ), 'prev_256 update failed'

    assert len(test_observable.metrics.history) == 0, 'wrong history, client is not None'


@patch('caom2pipe.client_composable.ClientCollection')
@patch('omm2caom2.cleanup_augmentation._send_slack_message')
def test_cleanup(slack_mock, client_mock, test_data_dir):
    test_obs_id = 'C090219_0001'
    test_obs_fqn = f'{test_data_dir}/{test_obs_id}_start.xml'
    test_obs = read_obs_from_file(test_obs_fqn)
    client_mock.data_client.info.side_effect = _mock_file_info
    kwargs = {'clients': client_mock}
    test_result = cleanup_augmentation.visit(test_obs, **kwargs)
    assert test_result is not None, 'expect a result'
    assert (
        f'{test_obs_id}_SCI' not in test_result.planes.keys()
    ), 'deleted the wrong one'
    assert (
        f'{test_obs_id}_REJECT' in test_result.planes.keys()
    ), 'deleted the other wrong one'
    assert slack_mock.called, 'mock should be called'
    assert slack_mock.call_count == 1, 'should only be one deletion notice'


def _mock_file_info(uri):
    sci_result = FileInfo(id=uri, lastmod='Fri, 28 Dec 2018 01:43:28 GMT')
    reject_result = FileInfo(id=uri, lastmod='Thu, 14 May 2020 20:29:02 GMT')
    result = reject_result
    if '_SCI' in uri:
        result = sci_result
    return result
