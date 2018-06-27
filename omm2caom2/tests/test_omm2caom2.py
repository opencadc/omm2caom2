from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from omm2caom2 import main_app
from caom2 import ObservationReader
from caom2.diff import get_differences

from hashlib import md5
import os
import sys

from mock import patch

TEST_URI = 'ad:OMM/imm_file.fits'

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(THIS_DIR, 'data')
PLUGIN = os.path.join(os.path.dirname(THIS_DIR), 'omm2caom2.py')


def pytest_generate_tests(metafunc):
    files = [os.path.join(TESTDATA_DIR, name) for name in
             os.listdir(TESTDATA_DIR) if name.endswith('header')]
    metafunc.parametrize('test_name', files)


def test_main_app(test_name):
    basename = os.path.basename(test_name)
    product_id = basename.split('.fits')[0]
    lineage = _get_lineage(product_id, basename)
    output_file = '{}.actual.xml'.format(test_name)
    local = _get_local(test_name)
    plugin = PLUGIN

    with patch('caom2utils.fits2caom2.CadcDataClient') as data_client_mock:
        def get_file_info(archive, file_id):
            if '_prev' in file_id:
                return {'size': 10290,
                        'md5sum': md5('-37'.encode()).hexdigest(),
                        'type': 'image/jpeg'}
            else:
                return {'size': 37,
                        'md5sum': md5('-37'.encode()).hexdigest(),
                        'type': 'application/octet-stream'}
        data_client_mock.return_value.get_file_info.side_effect = \
            get_file_info

        sys.argv = \
            ('omm2caom2 --no_validate --local {} '
             '--plugin {} --observation OMM {} -o {} --lineage {}'.
             format(local, plugin, product_id, output_file,
                    lineage)).split()
        print(sys.argv)
        main_app()
        obs_path = test_name.replace('header', 'xml')
        expected = _read_obs(obs_path)
        actual = _read_obs(output_file)
        result = get_differences(expected, actual, 'Observation')
        if result:
            msg = 'Differences found in observation {}\n{}'. \
                format(expected.observation_id, '\n'.join(
                [r for r in result]))
            raise AssertionError(msg)
        # assert False  # cause I want to see logging messages


def _read_obs(fname):
    assert os.path.exists(fname), fname
    reader = ObservationReader(False)
    result = reader.read(fname)
    return result


def _get_local(test_name):
    prev_name = test_name.replace('.fits.header', '_prev.jpg')
    prev_256_name = test_name.replace('.fits.header', '_prev_256.jpg')
    return '{} {} {}'.format(test_name, prev_name, prev_256_name)


def _get_lineage(product_id, basename):
    return '{}/ad:OMM/{}.fits.gz'.format(product_id, product_id)
