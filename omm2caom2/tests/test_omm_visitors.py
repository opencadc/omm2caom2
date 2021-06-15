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
import os
import pytest

from mock import Mock, patch

from caom2 import ChecksumURI
from omm2caom2 import footprint_augmentation, preview_augmentation
from omm2caom2 import OmmName, cleanup_augmentation
from caom2pipe import manage_composable as mc


THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TEST_DATA_DIR = os.path.join(THIS_DIR, 'data')
TEST_OBS = 'C170324_0054'
TEST_FILE = f'{TEST_OBS}_SCI.fits.gz'
TEST_FILES_DIR = '/test_files'


def test_footprint_aug_visit():
    with pytest.raises(AssertionError):
        footprint_augmentation.visit(None)


def test_footprint_update_position():
    omm_name = OmmName(file_name=TEST_FILE)
    test_kwargs = {'science_file': omm_name.file_name}
    test_fqn = os.path.join(
        TEST_DATA_DIR, f'{omm_name.product_id}.expected.xml'
    )
    test_obs = mc.read_obs_from_file(test_fqn)
    test_chunk = (
        test_obs.planes[omm_name.product_id]
        .artifacts[omm_name.file_uri]
        .parts['0']
        .chunks[0]
    )
    assert test_chunk.position.axis.bounds is None

    # expected failure due to required kwargs parameter
    with pytest.raises(mc.CadcException):
        test_result = footprint_augmentation.visit(test_obs)

    test_kwargs['working_directory'] = TEST_FILES_DIR
    test_result = footprint_augmentation.visit(test_obs, **test_kwargs)
    assert test_result is not None, 'expected a visit return value'
    assert test_result['chunks'] == 1
    assert (
        test_chunk.position.axis.bounds is not None
    ), 'bound calculation failed'


def test_preview_aug_visit():
    with pytest.raises(mc.CadcException):
        preview_augmentation.visit(None)


def test_preview_augment_plane():
    omm_name = OmmName(file_name=TEST_FILE)
    preview = os.path.join(TEST_FILES_DIR, omm_name.prev)
    thumb = os.path.join(TEST_FILES_DIR, omm_name.thumb)
    if os.path.exists(preview):
        os.remove(preview)
    if os.path.exists(thumb):
        os.remove(thumb)
    test_fqn = os.path.join(
        TEST_DATA_DIR, f'{omm_name.product_id}.expected.xml'
    )
    test_obs = mc.read_obs_from_file(test_fqn)
    assert len(test_obs.planes[omm_name.product_id].artifacts) == 1
    preva = 'ad:OMM/C170324_0054_SCI_prev.jpg'
    thumba = 'ad:OMM/C170324_0054_SCI_prev_256.jpg'

    test_config = mc.Config()
    test_config.observe_execution = True
    test_metrics = mc.Metrics(test_config)
    test_observable = mc.Observable(rejected=None, metrics=test_metrics)

    test_kwargs = {
        'working_directory': TEST_FILES_DIR,
        'cadc_client': None,
        'observable': test_observable,
        'stream': 'raw',
        'science_file': 'C170324_0054_SCI.fits.gz',
    }
    test_result = preview_augmentation.visit(test_obs, **test_kwargs)
    assert test_result is not None, 'expected a visit return value'
    assert test_result['artifacts'] == 2
    assert len(test_obs.planes[omm_name.product_id].artifacts) == 3
    assert os.path.exists(preview)
    assert os.path.exists(thumb)
    test_plane = test_obs.planes[omm_name.product_id]
    assert test_plane.artifacts[preva].content_checksum == ChecksumURI(
        'md5:de9f39804f172682ea9b001f8ca11f15'
    ), 'prev checksum failure'
    assert test_plane.artifacts[thumba].content_checksum == ChecksumURI(
        'md5:cd118dae04391f6bea93ba4bf2711adf'
    ), 'thumb checksum failure'

    # now do updates
    test_obs.planes[omm_name.product_id].artifacts[
        preva
    ].content_checksum = ChecksumURI('de9f39804f172682ea9b001f8ca11f15')
    test_obs.planes[omm_name.product_id].artifacts[
        thumba
    ].content_checksum = ChecksumURI('cd118dae04391f6bea93ba4bf2711adf')
    test_result = preview_augmentation.visit(test_obs, **test_kwargs)
    assert test_result is not None, 'expected update visit return value'
    assert test_result['artifacts'] == 2
    assert len(test_obs.planes) == 1
    assert len(test_obs.planes[omm_name.product_id].artifacts) == 3
    assert os.path.exists(preview)
    assert os.path.exists(thumb)
    assert test_plane.artifacts[preva].content_checksum == ChecksumURI(
        'md5:de9f39804f172682ea9b001f8ca11f15'
    ), 'prev update failed'
    assert test_plane.artifacts[thumba].content_checksum == ChecksumURI(
        'md5:cd118dae04391f6bea93ba4bf2711adf'
    ), 'prev_256 update failed'

    assert len(test_metrics.history) == 0, 'wrong history, client is not None'


@patch('omm2caom2.cleanup_augmentation._send_slack_message')
def test_cleanup(slack_mock):
    test_obs_id = 'C090219_0001'
    test_obs_fqn = f'{TEST_DATA_DIR}/{test_obs_id}_start.xml'
    test_obs = mc.read_obs_from_file(test_obs_fqn)
    cadc_client_mock = Mock()
    cadc_client_mock.get_file_info.side_effect = _mock_file_info
    kwargs = {'cadc_client': cadc_client_mock}
    test_result = cleanup_augmentation.visit(test_obs, **kwargs)
    assert test_result is not None, 'expect a result'
    assert 'planes' in test_result, 'wrong content'
    assert test_result.get('planes') == 1, 'wrong plane modification count'
    assert (
        f'{test_obs_id}_SCI' not in test_obs.planes.keys()
    ), 'deleted the wrong one'
    assert (
        f'{test_obs_id}_REJECT' in test_obs.planes.keys()
    ), 'deleted the other wrong one'
    assert slack_mock.called, 'mock should be called'
    assert slack_mock.call_count == 1, 'should only be one deletion notice'


def _mock_file_info(archive, f_name):
    sci_result = {'lastmod': 'Fri, 28 Dec 2018 01:43:28 GMT'}
    reject_result = {'lastmod': 'Thu, 14 May 2020 20:29:02 GMT'}
    result = reject_result
    if '_SCI' in f_name:
        result = sci_result
    return result
