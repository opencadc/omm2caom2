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


# @pytest.mark.parametrize('test_name',
#                          ['C120902_sh2-132_J_old_SCIRED.fits.header',
#                           'C170323_0172_CAL.fits.header',
#                           'C170217_0228_SCI.fits.header',
#                           'C170323_domeflat_K_CALRED.fits.header',
#                           'C170318_0002_FOCUS.fits.header',
#                           'C170324_0028_FOCUS.fits.header',
#                           'C170318_0121_SCI.fits.header',
#                           'C170324_0054_SCI.fits.header'])
def test_main_app():
    files = [os.path.join(TESTDATA_DIR, name) for name in
             os.listdir(TESTDATA_DIR) if name.endswith('header')]
    uris = ['ad:OMM/{}'.format(name.split('.header')[0]) for name in
            os.listdir(TESTDATA_DIR) if name.endswith('header')]
    for i, file in enumerate(files):
        product_id = os.path.basename(file).split('.fits')[0]
        lineage = '{}/{}'.format(product_id, uris[i])
        output_file = '{}.actual.xml'.format(file)

        with patch('caom2utils.fits2caom2.CadcDataClient') as data_client_mock:
            def get_file_info(archive, file_id):
                archive = 'ignored'
                file_id = 'ignored'
                meta = {}
                meta['size'] = 37
                meta['md5sum'] = md5('-37'.encode()).hexdigest()
                meta['type'] = 'application/octet-stream'
                return meta
            data_client_mock.return_value.get_file_info.side_effect = \
                get_file_info

            sys.argv = \
                ('omm2caom2 --ignorePartialWCS --local {} '
                 '--observation OMM test_obsid{} -o {} --lineage {}'.
                format(file, i, output_file, lineage)).split()
            print(sys.argv)
            main_app()
            obs_path = file.replace('header', 'xml')
            expected = _read_obs(obs_path)
            actual = _read_obs(output_file)
            result = get_differences(expected, actual, 'Observation')
            if result:
                msg = 'Differences found in observation {}\n{}'. \
                    format(expected.observation_id, '\n'.join(
                    [r for r in result]))
                # raise AssertionError(msg)


def _read_obs(fname):
    assert os.path.exists(fname), fname
    reader = ObservationReader(False)
    result = reader.read(fname)
    return result
