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

import pytest

from caom2pipe import manage_composable as mc
from omm2caom2 import OmmName, OmmBuilder


def test_is_valid():
    assert OmmName(file_name='C121212_00001_SCI.fits.gz').is_valid()
    assert not OmmName(file_name='c121212_00001_SCI.fits.gz').is_valid()
    assert OmmName(file_name='C121212_00001_CAL.fits.gz').is_valid()
    assert not OmmName(file_name='c121212_00001_CAL.fits.gz').is_valid()
    assert OmmName(file_name='C121212_domeflat_K_CALRED.fits.gz').is_valid()
    assert not OmmName(
        file_name='C121212_DOMEFLAT_K_CALRED.fits.gz'
    ).is_valid()
    assert OmmName(file_name='C121212_sh2-132_J_old_SCIRED.fits.gz').is_valid()
    assert OmmName(file_name='C121212_J0454+8024_J_SCIRED.fits.gz').is_valid()
    assert OmmName(file_name='C121212_00001_TEST.fits.gz').is_valid()
    assert OmmName(file_name='C121212_00001_FOCUS.fits.gz').is_valid()
    assert OmmName(
        file_name='C121121_J024345.57-021326.4_K_SCIRED.fits.gz'
    ).is_valid()

    test_subject = OmmName(file_name='C121212_00001_SCI.fits')
    assert test_subject.is_valid()
    assert test_subject.obs_id == 'C121212_00001'
    test_subject = OmmName(file_name='C121212_00001_SCI.fits.gz')
    assert test_subject.is_valid()
    assert test_subject.obs_id == 'C121212_00001'
    test_subject = OmmName(
        fname_on_disk='C121212_00001_SCI.fits',
        file_name='C121212_00001_SCI.fits.gz',
    )
    assert test_subject.is_valid()
    assert test_subject.obs_id == 'C121212_00001'

    with pytest.raises(mc.CadcException):
        test_subject = OmmName(file_name='C121212_00001_SCI')
        test_subject = OmmName(fname_on_disk='C121212_00001_FOCUS')
        test_subject = OmmName('C121212_00001_FOCUS.fits')
        test_subject = OmmName('C121212_00001_FOCUS.fits.gz')


def test_omm_name():
    test_config = mc.Config()
    test_config.task_types = []
    test_config.use_local_files = True
    test_builder = OmmBuilder(test_config)
    test_name = 'C121212_00001_SCI'
    for entry in [f'{test_name}', f'/tmp/{test_name}']:
        test_subject = test_builder.build(f'{entry}.fits')
        assert f'cadc:OMM/{test_name}.fits.gz' == test_subject.file_uri
        assert (
            test_subject.source_names == [f'{entry}.fits']
        ), 'wrong source name'
        assert (
            test_subject.destination_uris[0] ==
            f'cadc:OMM/{test_name}.fits.gz'
        ), 'wrong source name'

    test_name = 'C121212_sh2-132_J_old_SCIRED'
    file_name = f'{test_name}_prev_256.jpg'
    assert f'{test_name}_prev.jpg' == OmmName(file_name=file_name).prev
    assert f'{test_name}_prev_256.jpg' == OmmName(file_name=file_name).thumb
    test_name = 'C121212_sh2-132_J_old_SCIRED'
    file_name = f'{test_name}_prev_256.jpg'
    assert f'{test_name}_prev.jpg' == OmmName(file_name=file_name).prev
    assert f'{test_name}_prev_256.jpg' == OmmName(file_name=file_name).thumb
    test_obs_id = 'C121121_J024345.57-021326.4_K'
    test_name = f'{test_obs_id}_SCIRED'
    file_name = f'{test_name}.fits.gz'
    assert f'{test_name}_prev.jpg' == OmmName(file_name=file_name).prev
    assert f'{test_name}_prev_256.jpg' == OmmName(file_name=file_name).thumb
    assert OmmName(file_name=file_name).obs_id == test_obs_id
