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

from mock import Mock

from omm2caom2 import omm_footprint_augmentation, omm_preview_augmentation
from omm2caom2 import manage_composable, OmmName
from caom2 import ObservationReader


THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(THIS_DIR, 'data')
TEST_OBS = 'C170324_0054_SCI'


def test_footprint_aug_visit():
    try:
        omm_footprint_augmentation.visit(None)
    except AssertionError as e:
        return  # expected


def test_footprint_update_position():
    test_kwargs = {'science_file': OmmName(TEST_OBS).get_file_name()}
    test_obs = _read_obs_from_file()
    test_chunk = test_obs.planes[TEST_OBS].artifacts[
        OmmName(TEST_OBS).get_file_uri()].parts['0'].chunks[0]
    assert test_chunk.position.axis.bounds is None
    try:
        test_result = omm_footprint_augmentation.visit(test_obs)
    except manage_composable.CadcException as e:
        # expected failure due to required kwargs parameter
        pass
    try:
        test_result = omm_footprint_augmentation.visit(test_obs, **test_kwargs)
    except manage_composable.CadcException as e:
        # expected failure due to non-existent file
        pass
    test_kwargs['working_directory'] = TESTDATA_DIR
    test_result = omm_footprint_augmentation.visit(test_obs, **test_kwargs)
    assert test_result is not None, 'expected a visit return value'
    assert test_result['chunks'] == 1
    assert test_chunk.position.axis.bounds is not None, \
        'bound calculation failed'


def test_preview_aug_visit():
    try:
        omm_preview_augmentation.visit(None)
    except AssertionError as e:
        return  # expected


def test_preview_augment_plane():
    put_omm_orig = omm_preview_augmentation._put_omm_preview
    omm_preview_augmentation._put_omm_preview = Mock()
    preview = os.path.join(TESTDATA_DIR, OmmName(TEST_OBS).get_prev())
    thumb = os.path.join(TESTDATA_DIR, OmmName(TEST_OBS).get_thumb())
    if os.path.exists(preview):
        os.remove(preview)
    if os.path.exists(thumb):
        os.remove(thumb)
    test_obs = _read_obs_from_file()
    assert len(test_obs.planes[TEST_OBS].artifacts) == 1
    try:
        test_result = omm_preview_augmentation.visit(test_obs)
    except manage_composable.CadcException as e:
        # expected failure due to unspecified kwargs value for test_netrc
        pass
    test_kwargs = {'netrc_fqn': os.path.join(TESTDATA_DIR, 'test_netrc')}
    try:
        test_result = omm_preview_augmentation.visit(test_obs, **test_kwargs)
    except manage_composable.CadcException as e:
        # expected failure due to non-existent file
        pass
    test_kwargs['working_directory'] = TESTDATA_DIR
    test_result = omm_preview_augmentation.visit(test_obs, **test_kwargs)
    assert test_result is not None, 'expected a visit return value'
    assert test_result['artifacts'] == 2
    assert len(test_obs.planes[TEST_OBS].artifacts) == 3
    assert os.path.exists(preview)
    assert os.path.exists(thumb)
    assert omm_preview_augmentation._put_omm_preview.called
    omm_preview_augmentation._put_omm_preview = put_omm_orig


def _read_obs_from_file():
    test_fqn = os.path.join(TESTDATA_DIR,
                            OmmName(TEST_OBS).get_model_file_name())
    assert os.path.exists(test_fqn), test_fqn
    reader = ObservationReader(False)
    test_obs = reader.read(test_fqn)
    return test_obs
