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

import importlib
import logging
import os
import re
import sys
import traceback

from astropy.io import fits

from caom2 import TargetType, ObservationIntentType, CalibrationLevel
from caom2 import ProductType, Observation, Chunk, CoordRange1D, RefCoord
from caom2 import CoordFunction1D, CoordAxis1D, Axis, TemporalWCS, SpectralWCS
from caom2 import ObservationURI, PlaneURI, TypedSet, CoordBounds1D
from caom2utils import ObsBlueprint, get_gen_proc_arg_parser, gen_proc
# from caom2utils import augment
from omm2caom2 import astro_composable, manage_composable


__all__ = ['main_app', 'update', 'OmmName', 'CaomName']


# map the fits file values to the DataProductType enums
DATATYPE_LOOKUP = {'CALIB': 'flat',
                   'SCIENCE': 'object',
                   'FOCUS': 'focus',
                   'REDUC': 'reduc',
                   'TEST': 'test',
                   'REJECT': 'reject',
                   'CALRED': 'flat'}


class OmmName(object):
    """OMM naming rules:
    - support mixed-case file name storage
    - support gzipped and not zipped file names"""

    def __init__(self, obs_id):
        self.obs_id = obs_id

    OMM_NAME_PATTERN = 'C[\w\+\-]+[SCI|CAL|SCIRED|CALRED|TEST|FOCUS]'

    # TODO - MOVE THIS
    def get_file_uri(self):
        return 'ad:OMM/{}.gz'.format(self.get_file_name())

    def get_file_name(self):
        return '{}.fits'.format(self.obs_id)

    def get_compressed_file_name(self):
        return '{}.fits.gz'.format(self.obs_id)

    def get_model_file_name(self):
        return '{}.fits.xml'.format(self.obs_id)

    def get_prev(self):
        return '{}_prev.jpg'.format(self.obs_id)

    def get_thumb(self):
        return '{}_prev_256.jpg'.format(self.obs_id)

    def get_prev_uri(self):
        return self._get_uri(self.get_prev())

    def get_thumb_uri(self):
        return self._get_uri(self.get_thumb())

    def get_obs_id(self):
        return self.obs_id

    def get_log_file(self):
        return '{}.log'.format(self.obs_id)

    @staticmethod
    def _get_uri(fname):
        return 'ad:OMM/{}'.format(fname)

    @staticmethod
    def remove_extensions(name):
        return name.replace('.fits', '').replace('.gz', '').replace('.header',
                                                                    '')

    @staticmethod
    def is_valid(name):
        pattern = re.compile(OmmName.OMM_NAME_PATTERN)
        return pattern.match(name)

    @staticmethod
    def is_composite(uri):
        return '_SCIRED' in uri or '_CALRED' in uri


class CaomName(object):

    def __init__(self, uri):
        self.uri = uri

    def get_file_id(self):
        return self.uri.split('/')[1].split('.')[0]

    def get_file_name(self):
        return self.uri.split('/')[1]

    def get_uncomp_file_name(self):
        return self.get_file_name().replace('.gz', '')


def accumulate_obs(bp, uri):
    """Configure the OMM-specific ObsBlueprint at the CAOM model Observation
    level."""
    logging.debug('Begin accumulate_obs.')
    bp.set('Observation.type', 'get_obs_type(header)')
    bp.set('Observation.intent', 'get_obs_intent(header)')
    bp.add_fits_attribute('Observation.instrument.name', 'INSTRUME')
    bp.add_fits_attribute('Observation.instrument.keywords', 'DETECTOR')
    bp.set('Observation.instrument.keywords', 'DETECTOR=CPAPIR-HAWAII-2')
    bp.add_fits_attribute('Observation.target.name', 'OBJECT')
    bp.set('Observation.target.type', TargetType.OBJECT)
    bp.set('Observation.target.standard', False)
    bp.set('Observation.target.moving', False)
    bp.add_fits_attribute('Observation.target_position.point.cval1', 'RA')
    bp.add_fits_attribute('Observation.target_position.point.cval2', 'DEC')
    bp.set('Observation.target_position.coordsys', 'ICRS')
    bp.add_fits_attribute('Observation.target_position.equinox', 'EQUINOX')
    bp.set_default('Observation.target_position.equinox', '2000.0')
    bp.add_fits_attribute('Observation.telescope.name', 'TELESCOP')
    bp.set('Observation.telescope.geoLocationX', 'get_telescope_x(header)')
    bp.set('Observation.telescope.geoLocationY', 'get_telescope_y(header)')
    bp.set('Observation.telescope.geoLocationZ', 'get_telescope_z(header)')
    bp.add_fits_attribute('Observation.telescope.keywords', 'OBSERVER')
    bp.set('Observation.environment.ambientTemp',
           'get_obs_env_ambient_temp(header)')
    if OmmName.is_composite(uri):
        bp.add_table_attribute('CompositeObservation.members', 'FICS',
                               extension=1, index=0)


def accumulate_plane(bp):
    """Configure the OMM-specific ObsBlueprint at the CAOM model Plane
    level."""
    logging.debug('Begin accumulate_plane.')
    bp.set('Plane.dataProductType', 'image')
    bp.set('Plane.calibrationLevel', 'get_plane_cal_level(header)')
    bp.add_fits_attribute('Plane.provenance.name', 'INSTRUME')
    bp.add_fits_attribute('Plane.provenance.runID', 'NIGHTID')
    bp.add_fits_attribute('Plane.metaRelease', 'DATE-OBS')
    bp.set('Plane.provenance.version', '1.0')
    bp.set('Plane.provenance.reference', 'http://genesis.astro.umontreal.ca')
    bp.set('Plane.provenance.project', 'Standard Pipeline')


def accumulate_artifact(bp):
    """Configure the OMM-specific ObsBlueprint at the CAOM model Artifact
    level."""
    logging.debug('Begin accumulate_artifact.')
    bp.set('Artifact.productType', 'get_product_type(header)')


def accumulate_part(bp):
    """Configure the OMM-specific ObsBlueprint at the CAOM model Part
    level."""
    logging.debug('Begin accumulate part.')
    bp.set('Part.productType', 'get_product_type(header)')


def accumulate_position(bp):
    """Configure the OMM-specific ObsBlueprint for the CAOM model
    SpatialWCS."""
    logging.debug('Begin accumulate_position.')
    bp.configure_position_axes((1, 2))
    bp.set('Chunk.position.coordsys', 'ICRS')
    bp.set('Chunk.position.axis.error1.rnder',
           'get_position_resolution(header)')
    bp.set('Chunk.position.axis.error1.syser', 0.0)
    bp.set('Chunk.position.axis.error2.rnder',
           'get_position_resolution(header)')
    bp.set('Chunk.position.axis.error2.syser', 0.0)
    bp.set('Chunk.position.axis.axis1.ctype', 'RA---TAN')
    bp.set('Chunk.position.axis.axis2.ctype', 'DEC--TAN')
    bp.set('Chunk.position.axis.axis1.cunit', 'deg')
    bp.set('Chunk.position.axis.axis2.cunit', 'deg')


def get_end_ref_coord_val(header):
    """Calculate the upper bound of the spectral energy coordinate from
    FITS header values.

    Called to fill a blueprint value, must have a
    parameter named header for import_module loading and execution.

    :param header Array of astropy headers"""
    wlen = header[0].get('WLEN')
    bandpass = header[0].get('BANDPASS')
    if wlen is not None and bandpass is not None:
        return wlen + bandpass / 2.
    else:
        return None


def get_obs_type(header):
    """Calculate the Observation-level data type from FITS header values.

    Called to fill a blueprint value, must have a
    parameter named header for import_module loading and execution.

    :param header Array of astropy headers"""
    obs_type = None
    datatype = header[0].get('DATATYPE')
    if datatype in DATATYPE_LOOKUP:
        obs_type = DATATYPE_LOOKUP[datatype]
    return obs_type


def get_obs_intent(header):
    """Calculate the Observation-level intent from FITS header values.

    Called to fill a blueprint value, must have a
    parameter named header for import_module loading and execution.

    :param header Array of astropy headers"""
    lookup = ObservationIntentType.CALIBRATION
    datatype = header[0].get('DATATYPE')
    if 'SCIENCE' in datatype or 'REDUC' in datatype:
        lookup = ObservationIntentType.SCIENCE
    return lookup


def get_obs_env_ambient_temp(header):
    """Calculate the ambient temperature from FITS header values. Ignore
    what is used for default values, if they exist.

    Called to fill a blueprint value, must have a
    parameter named header for import_module loading and execution.

    :param header Array of astropy headers"""
    lookup = header[0].get('TEMP_WMO')
    if ((isinstance(lookup, float) or isinstance(lookup,
                                                 int)) and lookup < -99.):
        lookup = None
    return lookup


def get_plane_cal_level(header):
    """Calculate the Plane-level calibration level from FITS header values.

    Called to fill a blueprint value, must have a
    parameter named header for import_module loading and execution.

    :param header Array of astropy headers"""
    lookup = CalibrationLevel.RAW_STANDARD
    datatype = header[0].get('DATATYPE')
    if 'REDUC' in datatype:
        lookup = CalibrationLevel.CALIBRATED
    return lookup


def get_product_type(header):
    """Calculate the Plane-level, Artifact-level, Part-level, and Chunk-level
     product type from FITS header values.

    Called to fill a blueprint value, must have a
    parameter named header for import_module loading and execution.

    :param header Array of astropy headers"""
    lookup = ProductType.CALIBRATION
    datatype = header[0].get('DATATYPE')
    if 'SCIENCE' in datatype or 'REDUC' in datatype:
        lookup = ProductType.SCIENCE
    return lookup


def get_position_resolution(header):
    """Calculate the Plane-level position RNDER values from other FITS header
    values. Ignore values used by the telescope as defaults.

    Called to fill a blueprint value, must have a
    parameter named header for import_module loading and execution.

    :param header Array of astropy headers"""
    temp = None
    temp_astr = manage_composable.to_float(header[0].get('RMSASTR'))
    if temp_astr != -1.0:
        temp = temp_astr
    temp_mass = manage_composable.to_float(header[0].get('RMS2MASS'))
    if temp_mass != -1.0:
        temp = temp_mass
    return temp


def get_start_ref_coord_val(header):
    """Calculate the lower bound of the spectral energy coordinate from
    FITS header values.

    Called to fill a blueprint value, must have a
    parameter named header for import_module loading and execution.

    :param header Array of astropy headers"""
    wlen = header[0].get('WLEN')
    bandpass = header[0].get('BANDPASS')
    if wlen is not None and bandpass is not None:
        return wlen - bandpass / 2.
    else:
        return None


def get_telescope_x(header):
    """Calculate the telescope x coordinate in geocentric m from FITS header
    values.

    Called to fill a blueprint value, must have a
    parameter named header for import_module loading and execution.

    :param header Array of astropy headers"""
    telescope = header[0].get('TELESCOP')
    if telescope is None:
        return None
    if 'OMM' in telescope:
        return 1448027.602
    elif 'CTIO' in telescope:
        return 1815596.320
    return None


def get_telescope_y(header):
    """Calculate the telescope y coordinate in geocentric m from FITS header
    values.

    Called to fill a blueprint value, must have a
    parameter named header for import_module loading and execution.

    :param header Array of astropy headers"""
    telescope = header[0].get('TELESCOP')
    if telescope is None:
        return None
    if 'OMM' in telescope:
        return -4242089.498
    elif 'CTIO' in telescope:
        return -5213682.445
    return None


def get_telescope_z(header):
    """Calculate the telescope elevation in geocentric m from FITS header
    values.

    Called to fill a blueprint value, must have a
    parameter named header for import_module loading and execution.

    :param header Array of astropy headers"""
    telescope = header[0].get('TELESCOP')
    if telescope is None:
        return None
    if 'OMM' in telescope:
        return 4523791.027
    elif 'CTIO' in telescope:
        return -3187689.895
    return None


def update(observation, **kwargs):
    """Called to fill multiple CAOM model elements and/or attributes, must
    have this signature for import_module loading and execution.

    :param observation A CAOM Observation model instance.
    :param **kwargs Everything else."""
    logging.debug('Begin update.')

    assert observation, 'non-null observation parameter'
    assert isinstance(observation, Observation), \
        'observation parameter of type Observation'

    if 'headers' in kwargs:
        headers = kwargs['headers']

        for plane in observation.planes:
            for artifact in observation.planes[plane].artifacts:
                parts = observation.planes[plane].artifacts[artifact].parts
                for part in parts:
                    p = parts[part]
                    if len(p.chunks) == 0 and part == '0':
                        # always have a time axis, and usually an energy
                        # axis as well, so create a chunk for the zero-th part
                        p.chunks.append(Chunk())

                    for chunk in p.chunks:
                        chunk.naxis = 4
                        chunk.product_type = get_product_type(headers)
                        _update_energy(chunk, headers)
                        _update_time(chunk, headers)
                        _update_position(chunk)

        if OmmName.is_composite(observation.observation_id):
            _update_provenance(observation)
            _update_time_bounds(observation)

    logging.debug('Done update.')
    return True


def _update_energy(chunk, headers):
    """Create SpectralWCS information using FITS headers, if available. If
    the WLEN and BANDPASS keyword values are set to the defaults, there is
    no energy information."""
    logging.debug('Begin _update_energy')
    assert isinstance(chunk, Chunk), 'Expecting type Chunk'
    wlen = headers[0].get('WLEN')
    bandpass = headers[0].get('BANDPASS')
    if (wlen is None or wlen < 0 or
            bandpass is None or bandpass < 0):
        chunk.energy = None
        chunk.energy_axis = None
        logging.debug(
            'Setting chunk energy to None because WLEN {} and '
            'BANDPASS {}'.format(wlen, bandpass))
    else:
        naxis = CoordAxis1D(Axis('WAVE', 'um'))
        start_ref_coord = RefCoord(0.5, get_start_ref_coord_val(headers))
        end_ref_coord = RefCoord(1.5, get_end_ref_coord_val(headers))
        naxis.range = CoordRange1D(start_ref_coord, end_ref_coord)
        chunk.energy = SpectralWCS(naxis, specsys='TOPCENT',
                                   ssysobs='TOPCENT', ssyssrc='TOPCENT',
                                   bandpass_name=headers[0].get('FILTER'))
        chunk.energy_axis = 3
        logging.debug('Setting chunk energy range (CoordRange1D).')


def _update_time(chunk, headers):
    """Create TemporalWCS information using FITS header information.
    This information should always be available from the file."""
    logging.error('Begin _update_time.')
    assert isinstance(chunk, Chunk), 'Expecting type Chunk'

    mjd_start = headers[0].get('MJD_STAR')
    mjd_end = headers[0].get('MJD_END')
    if mjd_start is None or mjd_end is None:
        mjd_start, mjd_end = astro_composable.find_time_bounds(headers)
    if mjd_start is None or mjd_end is None:
        chunk.time = None
        logging.debug('Cannot calculate mjd_start {} or mjd_end {}'.format(
            mjd_start, mjd_end))
    else:
        logging.debug(
            'Calculating range with start {} and end {}.'.format(
                mjd_start, mjd_start))
        start = RefCoord(0.5, mjd_start)
        end = RefCoord(1.5, mjd_end)
        time_cf = CoordFunction1D(1, headers[0].get('TEXP'), start)
        time_axis = CoordAxis1D(Axis('TIME', 'd'), function=time_cf)
        time_axis.range = CoordRange1D(start, end)
        chunk.time = TemporalWCS(time_axis)
        chunk.time.exposure = headers[0].get('TEXP')
        chunk.time.resolution = 0.1
        chunk.time.timesys = 'UTC'
        chunk.time.trefpos = 'TOPOCENTER'
        chunk.time_axis = 4
    logging.debug('Done _update_time.')


def _update_position(chunk):
    """Check that position information has been set appropriately.
    Reset to null if there's bad input data."""
    logging.debug('Begin _update_position')
    assert isinstance(chunk, Chunk), 'Expecting type Chunk'
    if (chunk.position is not None and chunk.position.axis is not None and
            chunk.position.axis.function is None):
        chunk.position = None
        chunk.position_axis_1 = None
        chunk.position_axis_2 = None
        logging.debug('Removing the partial position record from the chunk.')
    logging.debug('End _update_position')


def _update_provenance(observation):
    """The provenance information in the reduced product headers is not
    patterned according to the content of ad. Make the provenance information
    conform, at the Observation and Plane level."""
    logging.debug('Begin _update_provenance')

    obs_type = None
    for ii in DATATYPE_LOOKUP:
        if DATATYPE_LOOKUP[ii] == observation.type:
            obs_type = ii
            break
    if obs_type == 'REDUC':
        extension = '_SCI'
    elif obs_type == 'FLAT':
        extension = '_CAL'
    else:
        logging.debug(
            'Observation.type is {}. No naming addition.'.format(obs_type))
        return

    new_members = TypedSet(ObservationURI,)
    new_inputs = TypedSet(PlaneURI,)
    for member in observation.members:
        new_obs_id = 'C{}'.format(
            member.observation_id.replace('.fits.gz', extension))
        new_member = ObservationURI('caom:OMM/{}'.format(new_obs_id))
        new_members.add(new_member)

        # the product id is the same as the observation id for OMM
        new_input = PlaneURI.get_plane_uri(new_member, new_obs_id)
        new_inputs.add(new_input)

    # remove the wrongly formatted values
    while len(observation.members) > 0:
        observation.members.pop()

    observation.members.update(new_members)
    observation.planes[observation.observation_id].provenance.inputs.update(
        new_inputs)
    logging.debug('End _update_provenance')


def _update_time_bounds(observation):
    """Add chunk time bounds to the chunk from the first part, by
     referencing information from the second header. """

    lower_values = ''
    upper_values = ''
    with fits.open(
            '/usr/src/app/test_composite/Cdemo_ext2_SCIRED.fits') as fits_data:
        xtension = fits_data[1].header['XTENSION']
        extname = fits_data[1].header['EXTNAME']
        if 'BINTABLE' in xtension and 'PROVENANCE' in extname:
            for ii in fits_data[1].data[0]['STARTTIME']:
                lower_values = '{} {}'.format(ii, lower_values)
            for ii in fits_data[1].data[0]['DURATION']:
                upper_values = '{} {} '.format(ii, upper_values)
        else:
            raise manage_composable.CadcException(
                'Opened a composite file that does not match the '
                'expected profile (XTENSION=BINTABLE/EXTNAME=PROVENANCE). '
                '{} {}'.format(xtension, extname))

    for plane in observation.planes:
        for artifact in observation.planes[plane].artifacts:
            parts = observation.planes[plane].artifacts[artifact].parts
            for p in parts:
                if p == '0':
                    lower = lower_values.split()
                    upper = upper_values.split()
                    if len(lower) != len(upper):
                        raise manage_composable.CadcException(
                            'Cannot make RefCoords with inconsistent values.')
                    chunk = parts[p].chunks[0]
                    bounds = CoordBounds1D()
                    chunk.time.axis.bounds = bounds
                    for ii in range(len(lower)):
                        mjd_start, mjd_end = astro_composable.convert_time(
                            manage_composable.to_float(lower[ii]),
                            manage_composable.to_float(upper[ii]))
                        lower_refcoord = RefCoord(0.5, mjd_start)
                        upper_refcoord = RefCoord(1.5, mjd_end)
                        r = CoordRange1D(lower_refcoord, upper_refcoord)
                        bounds.samples.append(r)


def _build_blueprints(uri):
    """This application relies on the caom2utils fits2caom2 ObsBlueprint
    definition for mapping FITS file values to CAOM model element
    attributes. This method builds the OMM blueprint for a single
    artifact.

    The blueprint handles the mapping of values with cardinality of 1:1
    between the blueprint entries and the model attributes.

    :param uri The artifact URI for the file to be processed."""
    module = importlib.import_module(__name__)
    blueprint = ObsBlueprint(module=module)
    accumulate_position(blueprint)
    accumulate_obs(blueprint, uri)
    accumulate_plane(blueprint)
    accumulate_artifact(blueprint)
    accumulate_part(blueprint)
    blueprints = {uri: blueprint}
    return blueprints


def _get_uri(args):
    result = None
    if args.observation:
        result = OmmName(args.observation[1]).get_file_uri()
    elif args.local:
        obs_id = OmmName.remove_extensions(os.path.basename(args.local[0]))
        result = OmmName(obs_id).get_file_uri()
    else:
        raise manage_composable.CadcException(
            'Could not define uri from these ares {}'.format(args))
    return result


def main_app():
    args = get_gen_proc_arg_parser().parse_args()
    try:
        uri = _get_uri(args)
        blueprints = _build_blueprints(uri)
        gen_proc(args, blueprints)
    except Exception as e:
        logging.error('Failed omm2caom2 execution for {}.'.format(args))
        tb = traceback.format_exc()
        logging.error(tb)
        sys.exit(-1)

    logging.debug('Done omm2caom2 processing.')
