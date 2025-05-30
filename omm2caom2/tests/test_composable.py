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
import sys

from collections import deque
from logging import error
from traceback import format_exc

from cadcdata import FileInfo
from caom2 import SimpleObservation, Algorithm
from caom2pipe.data_source_composable import ListDirDataSourceRunnerMeta
from caom2pipe.manage_composable import CadcException, Config, read_obs_from_file, TaskType
from caom2pipe.run_composable import run_by_todo_runner_meta

from unittest.mock import ANY, call, patch, Mock

from omm2caom2 import composable, OmmName


@patch('caom2pipe.client_composable.ClientCollection')
@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
@patch('caom2pipe.execute_composable.OrganizeExecutes.do_one')
def test_run_single(run_mock, access_mock, client_mock, test_config, tmp_path, change_test_dir):
    test_config.change_working_directory(tmp_path.as_posix())
    access_mock.return_value = 'https://localhost'
    client_mock.return_value.metadata_client.return_value = None
    test_obs_id = 'C121121_J024345.57-021326.4_K'
    test_f_id = f'{test_obs_id}_SCIRED'
    test_f = f'{test_f_id}.fits.gz'

    test_config.proxy_file_name = 'test_proxy.fqn'
    with open(test_config.proxy_fqn, 'w') as f_out:
        f_out.write('test content')

    test_config.write_to_file(test_config)
    sys.argv = f'omm2caom2 {test_f} {test_config.proxy_fqn}'.split()
    composable._run_single()
    assert run_mock.called, 'should have been called'
    args, _ = run_mock.call_args
    test_storage = args[0]
    assert isinstance(test_storage, OmmName), type(test_storage)
    assert test_storage.obs_id == test_obs_id, 'wrong obs id'
    assert test_storage.file_name == test_f, 'wrong file name'


@patch('caom2pipe.execute_composable.CaomExecuteRunnerMeta._visit_meta')
@patch('caom2pipe.client_composable.ClientCollection')
def test_run_rc_todo(client_mock, exec_mock, test_config, tmp_path, change_test_dir):
    test_config.task_types = [TaskType.VISIT]
    test_config.change_working_directory(tmp_path.as_posix())
    test_config.logging_level = 'INFO'
    test_config.write_to_file(test_config)
    with open(test_config.work_fqn, 'w') as f:
        f.write('C121212_domeflat_K_CALRED.fits.gz\n')

    client_mock.return_value.metadata_client.read.side_effect = _mock_repo_read
    client_mock.return_value.metadata_client.create.side_effect = Mock()
    client_mock.return_value.metadata_client.update.side_effect = _mock_repo_update
    client_mock.return_value.data_client.info.side_effect = _mock_get_file_info
    exec_mock.side_effect = _mock_exec

    # execution
    test_result = composable._run()
    assert test_result == 0, 'wrong result'

    assert client_mock.return_value.metadata_client.read.called, 'repo read not called'
    assert client_mock.return_value.metadata_client.update.called, 'repo update not called'
    # default config file says visit only, so it's a MetaVisit implementation,
    # so no data transfer
    assert exec_mock.called, 'expect to be called'


# common definitions for the next two tests
uris = {
    'C170324_0054': FileInfo(
        'cadc:OMM/C170324_0054_SCI.fits',
        size=16793280,
        file_type='application/fits',
        md5sum='md5:0e3dd1bd509460333eb13e0ff07315f7',
    ),
}


def _check_uris(obs):
    file_info = uris.get(obs.observation_id)
    assert file_info is not None, 'wrong observation id'
    for plane in obs.planes.values():
        for artifact in plane.artifacts.values():
            if artifact.uri == file_info.id:
                assert (
                    artifact.content_type == file_info.file_type
                ), 'type'
                assert artifact.content_length == file_info.size, 'size'
                assert (
                    artifact.content_checksum.uri == file_info.md5sum
                ), 'md5'
                return

    assert False, f'observation id not found {obs.observation_id}'


def _mock_dir_list():
    result = deque()
    result.append(OmmName(source_names=['/test_files/C170324_0054_SCI.fits.gz']))
    return result


@patch('caom2pipe.client_composable.ClientCollection')
def test_run_compression(clients_mock, test_config, tmp_path, change_test_dir):
    # this test works with FITS files, not header-only versions of FITS
    # files, because it's testing the decompression/recompression cycle

    def _mock_read(p1, p2):
        fqn = f'{tmp_path}/logs/{p2}.xml'
        import logging
        logging.error(fqn)
        if os.path.exists(fqn):
            # mock the modify task
            return read_obs_from_file(fqn)
        else:
            # mock the ingest task
            return None

    clients_mock.return_value.metadata_client.read.side_effect = (
        _mock_read
    )

    test_config.change_working_directory(tmp_path.as_posix())
    test_config.task_types = [
        TaskType.STORE,
        TaskType.INGEST,
        TaskType.MODIFY,
    ]
    test_config.logging_level = 'DEBUG'
    test_config.log_to_file = True
    test_config.proxy_file_name = 'cadcproxy.pem'
    test_config.use_local_files = True
    test_config.data_sources = ['/test_files']
    test_config.retry_failures = False
    test_config.cleanup_files_when_storing = False
    test_config.recurse_data_sources = False
    test_config.write_to_file(test_config)
    with open(test_config.proxy_fqn, 'w') as f:
        f.write('test content')
    # execution
    try:
        test_source = ListDirDataSourceRunnerMeta(test_config, chooser=None, storage_name_ctor=OmmName)
        test_source.get_work = Mock(side_effect=_mock_dir_list)
        test_result = run_by_todo_runner_meta(
            config=test_config,
            meta_visitors=composable.META_VISITORS,
            data_visitors=composable.DATA_VISITORS,
            sources=[test_source],
            storage_name_ctor=OmmName,
        )
        assert test_result == 0, 'expecting correct execution'
    except Exception as e:
        error(e)
        error(format_exc())
        raise e

    clients_mock.return_value.data_client.put.assert_called(), 'put'
    assert clients_mock.return_value.data_client.put.call_count == 3, 'put call count, including the previews'
    put_calls = [
        call(
            f'{tmp_path}/C170324_0054',
            f'{test_config.scheme}:{test_config.collection}/C170324_0054_SCI.fits',
        ),
        call(
            f'{tmp_path}/C170324_0054',
            f'{test_config.scheme}:{test_config.collection}/C170324_0054_SCI_prev.jpg',
        ),
        call(
            f'{tmp_path}/C170324_0054',
            f'{test_config.scheme}:{test_config.collection}/C170324_0054_SCI_prev_256.jpg',
        ),
    ]
    clients_mock.return_value.data_client.put.assert_has_calls(put_calls), 'wrong put args'
    assert not clients_mock.return_value.data_client.info.called, 'info'
    assert not clients_mock.return_value.data_client.get_head.called, 'LocalStore, get_head should not be called'
    assert not clients_mock.return_value.data_client.get.called, 'LocalStore, get should not be called'

    assert clients_mock.return_value.metadata_client.read.called, 'read'
    assert clients_mock.return_value.metadata_client.read.call_count == 1, 'meta read call ingest + modify'
    read_calls = [call('OMM', 'C170324_0054')]
    clients_mock.return_value.metadata_client.read.assert_has_calls(read_calls), 'wrong read args'

    assert clients_mock.return_value.metadata_client.create.called, 'create'
    assert clients_mock.return_value.metadata_client.create.call_count == 1, 'meta create call count'
    create_calls = [call(ANY)]
    clients_mock.return_value.metadata_client.create.assert_has_calls(create_calls), 'wrong create args'

    test_obs = read_obs_from_file(f'{test_config.working_directory}/logs/C170324_0054.xml')
    _check_uris(test_obs)


@patch('caom2pipe.client_composable.ClientCollection')
def test_run_compression_retry(clients_mock, tmp_path, change_test_dir):
    # this test works with FITS files, not header-only versions of FITS
    # files, because it's testing the decompression/recompression cycle

    clients_mock.return_value.data_client.put.side_effect = [
        CadcException, None, None, None
    ]

    def _mock_read(p1, p2):
        fqn = f'{tmp_path}/logs/{p2}.xml'
        fqn_0 = f'{tmp_path}/logs_0/{p2}.xml'
        if os.path.exists(fqn):
            # mock the modify task
            return read_obs_from_file(fqn)
        elif os.path.exists(fqn_0):
            # mock the modify task
            return read_obs_from_file(fqn_0)
        else:
            # mock the ingest task
            return None

    clients_mock.return_value.metadata_client.read.side_effect = (
        _mock_read
    )

    test_config = Config()
    test_config.change_working_directory(tmp_path.as_posix())
    test_config.task_types = [
        TaskType.STORE,
        TaskType.INGEST,
        TaskType.MODIFY,
    ]
    test_config.logging_level = 'INFO'
    test_config.log_to_file = True
    test_config.collection = 'OMM'
    test_config.proxy_file_name = 'cadcproxy.pem'
    test_config.use_local_files = True
    test_config.data_sources = ['/test_files']
    test_config.retry_failures = True
    test_config.retry_count = 1
    test_config.retry_decay = 0.01
    test_config.cleanup_files_when_storing = False
    test_config.recurse_data_sources = False
    Config.write_to_file(test_config)
    with open(test_config.proxy_fqn, 'w') as f:
        f.write('test content')
    # execution
    try:
        test_source = ListDirDataSourceRunnerMeta(test_config, chooser=None, storage_name_ctor=OmmName)
        test_source.get_work = Mock(side_effect=_mock_dir_list)
        test_result = run_by_todo_runner_meta(
            config=test_config,
            meta_visitors=composable.META_VISITORS,
            data_visitors=composable.DATA_VISITORS,
            sources=[test_source],
            storage_name_ctor=OmmName,
        )
        assert test_result == -1, 'retry expected, expected failure'
    except Exception as e:
        error(e)
        error(format_exc())
        raise e

    clients_mock.return_value.data_client.put.assert_called(), 'put'
    assert clients_mock.return_value.data_client.put.call_count == 4, 'put call count, including the previews'
    put_calls = [
        call(f'{tmp_path}/C170324_0054', 'cadc:OMM/C170324_0054_SCI.fits'),
        call(f'{tmp_path}/C170324_0054', 'cadc:OMM/C170324_0054_SCI.fits'),
        call(f'{tmp_path}/C170324_0054', 'cadc:OMM/C170324_0054_SCI_prev.jpg'),
        call(f'{tmp_path}/C170324_0054', 'cadc:OMM/C170324_0054_SCI_prev_256.jpg'),
    ]
    clients_mock.return_value.data_client.put.assert_has_calls(put_calls), 'wrong put args'

    assert not clients_mock.return_value.data_client.info.called, 'info'
    assert not clients_mock.return_value.data_client.get_head.called, 'LocalStore, get_head should not be called'
    assert not clients_mock.return_value.data_client.get.called, 'LocalStore, get should not be called'

    assert clients_mock.return_value.metadata_client.read.called, 'read'
    assert clients_mock.return_value.metadata_client.read.call_count == 1, 'meta read count ingest + modify'
    read_calls = [call('OMM', 'C170324_0054')]
    clients_mock.return_value.metadata_client.read.assert_has_calls(read_calls), 'wrong read args'

    assert clients_mock.return_value.metadata_client.create.called, 'create'
    assert clients_mock.return_value.metadata_client.create.call_count == 1, 'meta create call count'
    create_calls = [call(ANY)]
    clients_mock.return_value.metadata_client.create.assert_has_calls(create_calls), 'wrong create args'

    test_obs = read_obs_from_file(f'{test_config.working_directory}/logs_0/C170324_0054.xml')
    _check_uris(test_obs)


def _mock_repo_read(arg1, arg2):
    return _build_obs()


def _mock_repo_update(ignore1):
    return None


def _mock_exec():
    return _build_obs()


def _build_obs():
    return SimpleObservation(
        collection='TEST',
        observation_id='C121212_domeflat_K_CALRED',
        algorithm=Algorithm(name='test'),
    )


def _mock_get_file_info(ign):
    return FileInfo(id=ign)
