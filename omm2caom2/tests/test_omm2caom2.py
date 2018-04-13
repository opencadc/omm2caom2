from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from astropy.io import fits

from omm2caom2 import main_app
from caom2 import ObservationReader
from caom2.diff import get_differences

import logging
from hashlib import md5
import os
import pytest
import sys

from mock import patch, Mock

TEST_URI = 'ad:OMM/imm_file.fits'

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(THIS_DIR, 'data')

count = 0

def pytest_generate_tests(metafunc):
    files = [os.path.join(TESTDATA_DIR, name) for name in
             os.listdir(TESTDATA_DIR) if name.endswith('header')]
    metafunc.parametrize('test_name', files)


def test_main_app(test_name):
    basename = os.path.basename(test_name)
    product_id = basename.split('.fits')[0]
    lineage = '{}/ad:OMM/{}'.format(product_id, basename.split('.header')[0])
    output_file = '{}.actual.xml'.format(test_name)

    global count
    if count > 0:
        return

    with patch('caom2utils.fits2caom2.CadcDataClient') as data_client_mock:
        def get_file_info(archive, file_id):
           return {'size': 37,
                   'md5sum': md5('-37'.encode()).hexdigest(),
                   'type': 'application/octet-stream'}
        data_client_mock.return_value.get_file_info.side_effect = \
            get_file_info

        sys.argv = \
            ('omm2caom2 --debug --ignorePartialWCS --local {} '
             '--observation OMM {} -o {} --lineage {}'.
            format(test_name, product_id, output_file, lineage)).split()
        print(sys.argv)
        main_app()
        obs_path = test_name.replace('header', 'xml')
        expected = _read_obs(obs_path)
        actual = _read_obs(output_file)
        result = get_differences(expected, actual, 'Observation')
        count = 1
        if result:
            msg = 'Differences found in observation {}\n{}'. \
                format(expected.observation_id, '\n'.join(
                [r for r in result]))
            raise AssertionError(msg)
        # assert False # cause I want to see logging messages


def _read_obs(fname):
    assert os.path.exists(fname), fname
    reader = ObservationReader(False)
    result = reader.read(fname)
    return result
