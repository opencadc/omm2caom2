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
import numpy
import os
import sys
import traceback

from astropy.io import fits
import astropy.wcs as wcs

from caom2 import TargetType, ObservationIntentType, CalibrationLevel
from caom2 import ProductType, Observation, Chunk, CoordRange1D, RefCoord
from caom2 import CoordFunction1D, CoordAxis1D, Axis, TemporalWCS, SpectralWCS
from caom2 import ObservationURI, PlaneURI, TypedSet, CoordBounds1D
from caom2 import Requirements, Status, Instrument, Provenance
from caom2 import SimpleObservation, CompositeObservation, Algorithm
from caom2utils import ObsBlueprint, get_gen_proc_arg_parser, gen_proc
from caom2pipe import astro_composable as ac
from caom2pipe import manage_composable as mc
from caom2pipe import execute_composable as ec


__all__ = ['main_app', 'update', 'OmmName', 'COLLECTION', 'APPLICATION',
           '_update_cal_provenance', '_update_science_provenance',
           'OmmChooser']


APPLICATION = 'omm2caom2'
COLLECTION = 'OMM'

# map the fits file values to the DataProductType enums
DATATYPE_LOOKUP = {'CALIB': 'flat',
                   'SCIENCE': 'object',
                   'FOCUS': 'focus',
                   'REDUC': 'reduc',
                   'TEST': 'test',
                   'REJECT': 'reject',
                   'CALRED': 'flat'}

DEFAULT_GEOCENTRIC = {
    'OMM': {'x': 1448045.773, 'y': -4242075.462, 'z': 4523808.146,
            'elevation': 1108.},
    'CTIO': {'x': 1814303.745, 'y': -5214365.744, 'z': -3187340.566,
             'elevation': 2200.}}



class OmmName(ec.StorageName):
    """OMM naming rules:
    - support mixed-case file name storage, and mixed-case obs id values
    - support gzipped file names in ad, and gzipped and unzipped file names
      otherwise.
    - files are compressed in ad
    """

    OMM_NAME_PATTERN = 'C[\\w+-]+[SCI|CAL|SCIRED|CALRED|TEST|FOCUS]'

    def __init__(self, obs_id=None, fname_on_disk=None, file_name=None):
        if file_name is None and fname_on_disk is None:
            if obs_id.endswith('.fits') or obs_id.endswith('fits.gz'):
                raise mc.CadcException('Bad obs id value {}', obs_id)
            self.fname_in_ad = '{}.fits.gz'.format(obs_id)
        elif fname_on_disk is None:
            self.fname_in_ad = OmmName._add_extensions(file_name)
        elif file_name is None:
            self.fname_in_ad = OmmName._add_extensions(fname_on_disk)
        else:
            self.fname_in_ad = OmmName._add_extensions(fname_on_disk)
        if obs_id is None:
            obs_id = ec.StorageName.remove_extensions(self.fname_in_ad)

        super(OmmName, self).__init__(
            obs_id, COLLECTION, OmmName.OMM_NAME_PATTERN, fname_on_disk)

    def get_file_uri(self):
        return 'ad:{}/{}'.format(self.collection, self.fname_in_ad)

    def is_valid(self):
        result = False
        if super(OmmName, self).is_valid():
            if (self.obs_id.endswith('_SCIRED')
                    or self.obs_id.endswith('_CALRED')):
                obs_id_upper = self.obs_id.upper()
                if '_DOMEFLAT_' in obs_id_upper or '_DARK_' in obs_id_upper:
                    if '_domeflat_' in self.obs_id or '_dark_' in self.obs_id:
                        result = True
                else:
                    result = True
            else:
                result = True
        return result

    @staticmethod
    def _add_extensions(fname):
        if fname.endswith('.gz'):
            return fname
        elif fname.endswith('.fits'):
            return '{}.gz'.format(fname)
        else:
            raise mc.CadcException('Unexpected file name {}'.format(fname))

    @staticmethod
    def is_composite(uri):
        return '_SCIRED' in uri or '_CALRED' in uri


class OmmChooser(ec.OrganizeChooser):
    """OMM has the case where there are SimpleObservation instances that
    will change type to CompositeObservation instances. Tell the
    execute_composable package about that case."""

    def __init__(self,):
        super(OmmChooser, self).__init__()

    def needs_delete(self, observation):
        return (isinstance(observation, SimpleObservation) and
                OmmName.is_composite(observation.observation_id))


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
    bp.add_fits_attribute('Observation.telescope.keywords', 'OBSERVER')
    bp.set('Observation.environment.ambientTemp',
           'get_obs_env_ambient_temp(header)')
    if OmmName.is_composite(uri):
        bp.set('CompositeObservation.members', {})


def accumulate_plane(bp):
    """Configure the OMM-specific ObsBlueprint at the CAOM model Plane
    level."""
    logging.debug('Begin accumulate_plane.')
    bp.set('Plane.dataProductType', 'image')
    bp.set('Plane.calibrationLevel', 'get_plane_cal_level(header)')
    bp.add_fits_attribute('Plane.provenance.name', 'INSTRUME')
    bp.add_fits_attribute('Plane.provenance.runID', 'NIGHTID')
    bp.set('Plane.metaRelease', 'get_meta_release_date(header)')
    bp.set('Plane.dataRelease', 'get_data_release_date(header)')
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
    temp_astr = mc.to_float(header[0].get('RMSASTR'))
    if temp_astr != -1.0:
        temp = temp_astr
    temp_mass = mc.to_float(header[0].get('RMS2MASS'))
    if temp_mass != -1.0:
        temp = temp_mass
    return temp


def get_data_release_date(header):
    """Use the 'DATE' keyword for the release date, if the 'RELEASE' keyword
    does not exist.

    Called to fill a blueprint value, must have a
    parameter named header for import_module loading and execution.

    :param header Array of astropy headers"""
    rel_date = header[0].get('RELEASE')
    if rel_date is not None:
        return rel_date
    else:
        rel_date = header[0].get('DATE')
        return rel_date


def get_meta_release_date(header):
    """Use the 'DATE' keyword for the release date, if the 'DATE-OBS' keyword
    does not exist.

    Called to fill a blueprint value, must have a
    parameter named header for import_module loading and execution.

    :param header Array of astropy headers"""
    rel_date = header[0].get('DATE-OBS')
    if rel_date is not None:
        return rel_date
    else:
        rel_date = header[0].get('DATE')
        return rel_date


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


def update(observation, **kwargs):
    """Called to fill multiple CAOM model elements and/or attributes, must
    have this signature for import_module loading and execution.

    :param observation A CAOM Observation model instance.
    :param **kwargs Everything else."""
    logging.debug('Begin update.')
    mc.check_param(observation, Observation)

    headers = None
    if 'headers' in kwargs:
        headers = kwargs['headers']
    fqn = None
    if 'fqn' in kwargs:
        fqn = kwargs['fqn']

    _update_telescope_location(observation, headers)

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
                    _update_position(chunk, headers)

    if observation.observation_id.endswith('_REJECT'):
        _update_requirements(observation)

    if (observation.instrument is None or observation.instrument.name is None
            or len(observation.instrument.name) == 0):
        _update_instrument_name(observation)

    if OmmName.is_composite(observation.observation_id):
        if OmmChooser().needs_delete(observation):
            observation = _update_observation_type(observation)
        _update_provenance(observation, headers)
        # _update_time_bounds(observation, fqn)

    logging.debug('Done update.')
    return True


def _update_energy(chunk, headers):
    """Create SpectralWCS information using FITS headers, if available. If
    the WLEN and BANDPASS keyword values are set to the defaults, there is
    no energy information."""
    logging.debug('Begin _update_energy')
    mc.check_param(chunk, Chunk)

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


def _update_instrument_name(observation):
    """Hard-code instrument name, if it's not specified in header keywords."""
    if observation.observation_id.startswith('C'):
        name = 'CPAPIR'
    elif observation.observation_id.startswith('P'):
        name = 'PESTO'
    elif observation.observation_id.startswith('S'):
        name = 'SPIOMM'
    else:
        raise mc.CadcException('Unexpected observation id format: {}'.format(
            observation.observation_id))
    observation.instrument = Instrument(name)


def _update_observation_type(observation):
    """For the case where a SimpleObservation needs to become a
    CompositeObservation."""
    return CompositeObservation(observation.collection,
                                observation.observation_id,
                                observation.algorithm,
                                observation.sequence_number,
                                observation.intent,
                                Algorithm('composite'),
                                observation.proposal,
                                observation.telescope,
                                observation.instrument,
                                observation.target,
                                observation.meta_release,
                                observation.planes,
                                observation.environment,
                                observation.target_position)


def _update_time(chunk, headers):
    """Create TemporalWCS information using FITS header information.
    This information should always be available from the file."""
    logging.debug('Begin _update_time.')
    mc.check_param(chunk, Chunk)

    mjd_start = headers[0].get('MJD_STAR')
    mjd_end = headers[0].get('MJD_END')
    if mjd_start is None or mjd_end is None:
        mjd_start, mjd_end = ac.find_time_bounds(headers)
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
        time_cf = CoordFunction1D(1, headers[0].get('TEFF'), start)
        time_axis = CoordAxis1D(Axis('TIME', 'd'), function=time_cf)
        time_axis.range = CoordRange1D(start, end)
        chunk.time = TemporalWCS(time_axis)
        chunk.time.exposure = headers[0].get('TEFF')
        chunk.time.resolution = 0.1
        chunk.time.timesys = 'UTC'
        chunk.time.trefpos = 'TOPOCENTER'
        chunk.time_axis = 4
    logging.debug('Done _update_time.')


def _update_position(chunk, headers):
    """Check that position information has been set appropriately.
    Reset to null if there's bad input data."""
    logging.debug('Begin _update_position')
    mc.check_param(chunk, Chunk)

    w = wcs.WCS(headers[0])

    if ((chunk.position is not None and chunk.position.axis is not None and
         chunk.position.axis.function is None) or
            (numpy.allclose(w.wcs.crval[0], 0.) and
             numpy.allclose(w.wcs.crval[1], 0))):
        chunk.position = None
        chunk.position_axis_1 = None
        chunk.position_axis_2 = None
        logging.debug('Removing the partial position record from the chunk.')
    logging.debug('End _update_position')


def _update_provenance(observation, headers):
    """The provenance information in the reduced product headers is not
    patterned according to the content of ad. Make the provenance information
    conform, at the Observation and Plane level.

    In the CALRED files, the provenance information is available via
    header keywords.
    """
    logging.debug('Begin _update_provenance')

    if observation.observation_id.endswith('_SCIRED'):
        _update_science_provenance(observation, headers)
    else:
        _update_cal_provenance(observation, headers)

    logging.debug('End _update_provenance')


def _update_science_provenance(observation, headers):
    members_inputs = TypedSet(ObservationURI,)
    plane_inputs = TypedSet(PlaneURI,)
    for keyword in headers[0]:
        if keyword.startswith('IN_'):
            value = headers[0].get(keyword)
            base_name = OmmName.remove_extensions(os.path.basename(value))
            if base_name.startswith('S'):
                # starting 'S' means a science input, 'C' will mean cal
                file_id = '{}_SCI'.format(base_name.replace('S', 'C', 1))
            elif base_name.startswith('C'):
                file_id = '{}_CAL'.format(base_name)
            else:
                raise mc.CadcException(
                    'Unknown file naming pattern {}'.format(base_name))

            obs_member_uri_str = ec.CaomName.make_obs_uri_from_obs_id(
                COLLECTION, file_id)
            obs_member_uri = ObservationURI(obs_member_uri_str)
            # the product id is the same as the observation id for OMM
            plane_uri = PlaneURI.get_plane_uri(obs_member_uri, file_id)
            plane_inputs.add(plane_uri)
            members_inputs.add(obs_member_uri)

    mc.update_typed_set(observation.members, members_inputs)
    mc.update_typed_set(
        observation.planes[observation.observation_id].provenance.inputs,
        plane_inputs)


def _update_cal_provenance(observation, headers):
    plane_inputs = TypedSet(PlaneURI,)
    members_inputs = TypedSet(ObservationURI,)
    for keyword in headers[0]:
        if keyword.startswith('F_ON') or keyword.startswith('F_OFF'):
            value = headers[0].get(keyword)
            base_name = OmmName.remove_extensions(os.path.basename(value))
            file_id = 'C{}_CAL'.format(base_name)

            obs_member_uri_str = ec.CaomName.make_obs_uri_from_obs_id(
                COLLECTION, file_id)
            obs_member_uri = ObservationURI(obs_member_uri_str)
            # the product id is the same as the observation id for OMM
            plane_uri = PlaneURI.get_plane_uri(obs_member_uri, file_id)
            plane_inputs.add(plane_uri)
            members_inputs.add(obs_member_uri)

    for key in observation.planes:
        plane = observation.planes[key]
        if plane.provenance is None:
            plane.provenance = Provenance('CPAPIR')
        mc.update_typed_set(plane.provenance.inputs, plane_inputs)

    mc.update_typed_set(observation.members, members_inputs)


def _update_requirements(observation):
    """
    Add Requirements Status FAIL to observations that are named _REJECT.
    Hard-code the target name to 'BAD' in this case, as well.

    :param observation: Observation.requirements.status
    """
    observation.requirements = Requirements(Status.FAIL)
    observation.target.name = 'BAD'


def _update_time_bounds(observation, fqn):
    """Add chunk time bounds to the chunk from the first part, by
     referencing information from the second header. """

    lower_values = ''
    upper_values = ''
    with fits.open(fqn) as fits_data:
        xtension = fits_data[1].header['XTENSION']
        extname = fits_data[1].header['EXTNAME']
        if 'BINTABLE' in xtension and 'PROVENANCE' in extname:
            for ii in fits_data[1].data[0]['STARTTIME']:
                lower_values = '{} {}'.format(ii, lower_values)
            for ii in fits_data[1].data[0]['DURATION']:
                upper_values = '{} {} '.format(ii, upper_values)
        else:
            raise mc.CadcException(
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
                        raise mc.CadcException(
                            'Cannot make RefCoords with inconsistent values.')
                    chunk = parts[p].chunks[0]
                    bounds = CoordBounds1D()
                    chunk.time.axis.bounds = bounds
                    for ii in range(len(lower)):
                        mjd_start, mjd_end = ac.convert_time(
                            mc.to_float(lower[ii]),
                            mc.to_float(upper[ii]))
                        lower_refcoord = RefCoord(0.5, mjd_start)
                        upper_refcoord = RefCoord(1.5, mjd_end)
                        r = CoordRange1D(lower_refcoord, upper_refcoord)
                        bounds.samples.append(r)
                    # if execution has gotten to this point, remove range if
                    # it exists, since only one of bounds or range should be
                    # provided, and bounds is more specific. PD, slack,
                    # 2018-07-16
                    if chunk.time.axis.range is not None:
                        chunk.time.axis.range = None


def _update_telescope_location(observation, headers):
    """Provide geocentric telescope location information, based on
    geodetic information from the headers."""

    logging.debug('Begin _update_telescope_location')
    if not isinstance(observation, Observation):
        raise mc.CadcException('Input type is Observation.')

    telescope = headers[0].get('TELESCOP')

    if telescope is None:
        logging.warning('No telescope name. Could not set telescope '
                        'location for {}'.format(observation.observation_id))
        return

    telescope = telescope.upper()
    if COLLECTION in telescope or 'CTIO' in telescope:
        lat = headers[0].get('OBS_LAT')
        long = headers[0].get('OBS_LON')

        # make a reliable lookup value
        if COLLECTION in telescope:
            telescope = COLLECTION
        if 'CTIO' in telescope:
            telescope = 'CTIO'

        if lat is None or long is None:
            observation.telescope.geo_location_x = \
                DEFAULT_GEOCENTRIC[telescope]['x']
            observation.telescope.geo_location_y = \
                DEFAULT_GEOCENTRIC[telescope]['y']
            observation.telescope.geo_location_z = \
                DEFAULT_GEOCENTRIC[telescope]['z']
        else:
            observation.telescope.geo_location_x, \
                observation.telescope.geo_location_y, \
                observation.telescope.geo_location_z = \
                ac.get_location(
                    lat, long, DEFAULT_GEOCENTRIC[telescope]['elevation'])
    else:
        raise mc.CadcException(
            'Unexpected telescope name {}'.format(telescope))

    logging.debug('Done _update_telescope_location')


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
    elif args.lineage:
        result = args.lineage[0].split('/', 1)[1]
    else:
        raise mc.CadcException(
            'Could not define uri from these args {}'.format(args))
    return result


def main_app():
    args = get_gen_proc_arg_parser().parse_args()
    try:
        uri = _get_uri(args)
        blueprints = _build_blueprints(uri)
        gen_proc(args, blueprints)
    except Exception as e:
        logging.error('Failed {} execution for {}.'.format(APPLICATION, args))
        tb = traceback.format_exc()
        logging.error(tb)
        sys.exit(-1)

    logging.debug('Done {} processing.'.format(APPLICATION))
