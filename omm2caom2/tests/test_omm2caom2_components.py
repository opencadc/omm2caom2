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

from caom2utils import data_util
from caom2pipe import manage_composable as mc
from omm2caom2 import _update_cal_provenance, _update_science_provenance

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(THIS_DIR, 'data')


def test_update_cal_provenance():
    test_obs = 'C170323_domeflat_K_CALRED'
    test_obs_file = os.path.join(TESTDATA_DIR, f'{test_obs}.expected.xml')
    test_header_file = os.path.join(TESTDATA_DIR, f'{test_obs}.fits.header')
    test_obs = mc.read_obs_from_file(test_obs_file)
    headers = data_util.get_local_file_headers(test_header_file)
    _update_cal_provenance(test_obs, headers)
    assert test_obs is not None, 'no test_obs'
    assert test_obs.members is not None, 'no members'
    assert len(test_obs.members) == 22, 'wrong obs members length'
    test_member = test_obs.members.pop()
    assert test_member.uri.find('caom:OMM/C170323') == 0, 'wrong member value'
    assert test_member.uri.startswith('caom:OMM'), 'wrong member value'
    for ii in test_obs.planes:
        plane = test_obs.planes[ii]
        assert plane.provenance is not None, 'no provenance'
        assert plane.provenance.inputs is not None, 'no provenance inputs'
        assert (
            len(plane.provenance.inputs) == 22
        ), 'wrong provenance inputs length'
        test_plane_input = plane.provenance.inputs.pop()
        assert (
            test_plane_input.uri.find('caom:OMM/C170323') == 0
        ), 'wrong input value'
        assert test_plane_input.uri.endswith('_CAL'), 'wrong input value'


def test_update_sci_provenance():
    test_obs = 'C160929_NGC7419_K_SCIRED'
    test_obs_file = os.path.join(TESTDATA_DIR, f'{test_obs}.expected.xml')
    test_header_file = os.path.join(TESTDATA_DIR, f'{test_obs}.fits.header')
    test_obs = mc.read_obs_from_file(test_obs_file)
    headers = data_util.get_local_file_headers(test_header_file)
    _update_science_provenance(test_obs, headers)
    assert test_obs is not None, 'no test_obs'
    assert test_obs.members is not None, 'no members'
    assert len(test_obs.members) == 133, 'wrong obs members length'
    test_member = test_obs.members.pop()
    assert test_member.uri.find('caom:OMM/C160929') == 0, 'wrong member value'
    assert test_member.uri.startswith('caom:OMM'), 'wrong member value'
    for ii in test_obs.planes:
        plane = test_obs.planes[ii]
        assert plane.provenance is not None, 'no provenance'
        assert plane.provenance.inputs is not None, 'no provenance inputs'
        assert (
            len(plane.provenance.inputs) == 133
        ), 'wrong provenance inputs length'
        test_plane_input = plane.provenance.inputs.pop()
        assert (
            test_plane_input.uri.find('caom:OMM/C160929') == 0
        ), 'wrong input value'
        assert test_plane_input.uri.endswith('_SCI'), 'wrong input value'
