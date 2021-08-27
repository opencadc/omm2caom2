# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2019.                            (c) 2019.
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
import sys

from caom2 import SimpleObservation, Algorithm
from caom2pipe import manage_composable as mc

from unittest.mock import patch, Mock

from omm2caom2 import composable, OmmName
import test_main_app


STATE_FILE = '/usr/src/app/state.yml'
TODO_FILE = f'{test_main_app.TEST_DATA_DIR}/todo.txt'
PROGRESS_FILE = '/usr/src/app/logs/progress.txt'


@patch('caom2pipe.client_composable.ClientCollection')
@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
@patch('caom2pipe.execute_composable.OrganizeExecutes.do_one')
def test_run_single(run_mock, access_mock, client_mock):
    access_mock.return_value = 'https://localhost'
    client_mock.return_value.metadata_client.return_value = None
    test_obs_id = 'C121121_J024345.57-021326.4_K'
    test_f_id = f'{test_obs_id}_SCIRED'
    test_f = f'{test_f_id}.fits.gz'
    getcwd_orig = os.getcwd
    os.getcwd = Mock(return_value=test_main_app.TEST_DATA_DIR)
    test_proxy = f'{test_main_app.TEST_DATA_DIR}/cadcproxy.pem'
    try:
        # execution
        sys.argv = f'omm2caom2 {test_f} {test_proxy}'.split()
        composable._run_single()
        assert run_mock.called, 'should have been called'
        args, kwargs = run_mock.call_args
        test_storage = args[0]
        assert isinstance(test_storage, OmmName), type(test_storage)
        assert test_storage.obs_id == test_obs_id, 'wrong obs id'
        assert test_storage.file_name == test_f, 'wrong file name'
        assert test_storage.fname_on_disk is None, 'wrong fname on disk'
        assert test_storage.url is None, 'wrong url'
        assert (
            test_storage.lineage == f'{test_f_id}/cadc:OMM/{test_f}'
        ), 'wrong lineage'
        assert test_storage.external_urls is None, 'wrong external urls'
    finally:
        os.getcwd = getcwd_orig


@patch('caom2pipe.execute_composable.CaomExecute._visit_meta')
@patch('caom2pipe.client_composable.ClientCollection')
def test_run_rc_todo(client_mock, exec_mock):
    _write_todo('C121212_domeflat_K_CALRED.fits.gz')
    client_mock.return_value.metadata_client.read.side_effect = _mock_repo_read
    client_mock.return_value.metadata_client.create.side_effect = Mock()
    client_mock.return_value.metadata_client.update.side_effect = (
        _mock_repo_update
    )
    client_mock.return_value.data_client.info.side_effect = (
        test_main_app._mock_get_file_info
    )
    exec_mock.side_effect = _mock_exec
    getcwd_orig = os.getcwd
    os.getcwd = Mock(return_value=test_main_app.TEST_DATA_DIR)
    try:
        # execution
        test_result = composable._run()
        assert test_result == 0, 'wrong result'
    finally:
        os.getcwd = getcwd_orig

    assert (
        client_mock.return_value.metadata_client.read.called
    ), 'repo read not called'
    assert (
        client_mock.return_value.metadata_client.update.called
    ), 'repo update not called'
    # default config file says visit only, so it's a MetaVisit implementation,
    # so no data transfer
    assert exec_mock.called, 'expect to be called'


def _write_todo(test_obs_id):
    with open(TODO_FILE, 'w') as f:
        f.write(f'{test_obs_id}\n')


def _mock_repo_read(arg1, arg2):
    return _build_obs()


def _mock_repo_update(ignore1):
    return None


def _mock_exec(ignore1):
    obs = _build_obs()
    path = f'{test_main_app.TEST_DATA_DIR}/C121212_domeflat_K_CALRED'
    if not os.path.exists(path):
        os.mkdir(path)
    mc.write_obs_to_file(
        obs,
        f'{test_main_app.TEST_DATA_DIR}/'
        f'C121212_domeflat_K_CALRED/'
        f'C121212_domeflat_K_CALRED.fits.xml',
    )


def _build_obs():
    return SimpleObservation(
        collection='TEST',
        observation_id='C121212_domeflat_K_CALRED',
        algorithm=Algorithm(name='test'),
    )
