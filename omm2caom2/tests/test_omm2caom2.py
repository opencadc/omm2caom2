from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from astropy.io import fits

from omm2caom2 import main_app
from caom2 import ObservationReader
from caom2.diff import get_differences

import os
import pytest
import sys


TEST_URI = 'ad:OMM/imm_file.fits'

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(THIS_DIR, 'data')


@pytest.mark.parametrize('test_name', ['idontknow'])
def test_main_app(test_name):
    location = os.path.join(TESTDATA_DIR, test_name)
    actual_file_name = os.path.join(location, '{}.actual.xml'.format(test_name))
    files = ' '.join(
        [os.path.join(location, name) for name in os.listdir(location) if
         name.endswith('header')])
    uris = ' '.join(
        ['ad:OMM/{}'.format(name.split('.header')[0]) for name in
         os.listdir(location) if name.endswith('header')])
    sys.argv = \
        ('omm2caom2 --debug --observation OMM test_obsid -o {} {}'.
         format(files, test_name, actual_file_name, uris)).split()
    main_app()
    expected = _read_obs(os.path.join(location, '{}.xml'.format(test_name)))
    actual = _read_obs(actual_file_name)
    result = get_differences(expected, actual, 'Observation')
    if result:
        msg = 'Differences found in observation {} in {}\n{}'. \
            format(expected.observation_id,
                   location, '\n'.join([r for r in result]))
        raise AssertionError(msg)


def _read_obs(fname):
    assert os.path.exists(fname)
    reader = ObservationReader(False)
    result = reader.read(fname)
    return result
