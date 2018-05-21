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

import logging
import sys
import traceback

from datetime import datetime

from caom2 import TargetType, ObservationIntentType, CalibrationLevel
from caom2 import ProductType, Observation, Chunk, CoordRange1D, RefCoord
from caom2 import CoordFunction1D, CoordAxis1D, Axis, TemporalWCS, SpectralWCS
from caom2utils import ObsBlueprint, get_gen_proc_arg_parser, gen_proc
from caom2utils import gen_proc_no_args

from astropy.time import Time, TimeDelta

import importlib

__all__ = ['main_app', 'main_app_kwargs', 'update']

DATATYPE_LOOKUP = {'CALIB': 'flat',
                   'SCIENCE': 'object',
                   'FOCUS': 'focus',
                   'REDUC': 'reduc',
                   'TEST': 'test',
                   'REJECT': 'reject',
                   'CALRED': 'flat'}


def accumulate_obs(bp):
    """Configure the OMM-specific ObsBlueprint at the CAOM model Observation
    level."""
    logging.debug('Begin accumulate_obs.')
    bp.set('Observation.type', 'get_obs_type(header)')
    bp.set('Observation.intent', 'get_obs_intent(header)')
    bp.set_fits_attribute('Observation.instrument.name', ['INSTRUME'])
    bp.set_fits_attribute('Observation.instrument.keywords', ['DETECTOR'])
    bp.set('Observation.instrument.keywords', 'DETECTOR=CPAPIR-HAWAII-2')
    bp.set_fits_attribute('Observation.target.name', ['OBJECT'])
    bp.set('Observation.target.type', TargetType.OBJECT)
    bp.set('Observation.target.standard', False)
    bp.set('Observation.target.moving', False)
    bp.set_fits_attribute('Observation.target_position.point.cval1', ['RA'])
    bp.set_fits_attribute('Observation.target_position.point.cval2', ['DEC'])
    bp.set('Observation.target_position.coordsys', 'ICRS')
    bp.set_fits_attribute('Observation.target_position.equinox', ['EQUINOX'])
    bp.set_default('Observation.target_position.equinox', '2000.0')
    bp.set_fits_attribute('Observation.telescope.name', ['TELESCOP'])
    bp.set_fits_attribute('Observation.telescope.geoLocationX', ['OBS_LAT'])
    bp.set_fits_attribute('Observation.telescope.geoLocationY', ['OBS_LON'])
    bp.set('Observation.telescope.geoLocationZ', 'get_telescope_z(header)')
    bp.set_fits_attribute('Observation.telescope.keywords', ['OBSERVER'])
    bp.set('Observation.environment.ambientTemp',
           'get_obs_env_ambient_temp(header)')


def accumulate_plane(bp):
    """Configure the OMM-specific ObsBlueprint at the CAOM model Plane
    level."""
    logging.debug('Begin accumulate_plane.')
    bp.set('Plane.dataProductType', 'image')
    bp.set('Plane.calibrationLevel', 'get_plane_cal_level(header)')
    bp.set_fits_attribute('Plane.provenance.name', ['INSTRUME'])
    bp.set_fits_attribute('Plane.provenance.runID', ['NIGHTID'])
    bp.set_fits_attribute('Plane.metaRelease', ['DATE-OBS'])
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
    temp_astr = _to_float(header[0].get('RMSASTR'))
    if temp_astr != -1.0:
        temp = temp_astr
    temp_mass = _to_float(header[0].get('RMS2MASS'))
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

def get_telescope_z(header):
    """Calculate the telescope elevation from FITS header values.

    Called to fill a blueprint value, must have a
    parameter named header for import_module loading and execution.

    :param header Array of astropy headers"""
    telescope = header[0].get('TELESCOP')
    if telescope is None:
        return None
    if 'OMM' in telescope:
        return 1100.
    elif 'CTIO' in telescope:
        return 2200.
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

    for plane in observation.planes:
        for artifact in observation.planes[plane].artifacts:
            for part in observation.planes[plane].artifacts[artifact].parts:
                p = observation.planes[plane].artifacts[artifact].parts[part]
                if len(p.chunks) == 0:
                    # always have a time axis, and usually an energy axis as
                    # well, so create a chunk
                    part.chunks.append(Chunk())

                for chunk in p.chunks:
                    if 'headers' in kwargs:
                        chunk.naxis = 4
                        headers = kwargs['headers']
                        chunk.product_type = get_product_type(headers)
                        _update_energy(chunk, headers)
                        _update_time(chunk, headers)

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
    logging.debug('Begin _update_time.')
    assert isinstance(chunk, Chunk), 'Expecting type Chunk'

    mjd_start = headers[0].get('MJD_STAR')
    mjd_end = headers[0].get('MJD_END')
    if mjd_start is None or mjd_end is None:
        mjd_start, mjd_end = _convert_time(headers)
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


# TODO - this can be moved to 'the common library code'
def _convert_time(headers):
    logging.debug('Begin _convert_time.')
    date = headers[0].get('DATE-OBS')
    exposure = headers[0].get('TEXP')
    if date is not None and exposure is not None:
        logging.debug(
            'Use date {} and exposure {} to convert time.'.format(date,
                                                                  exposure))
        t_start = Time(_get_datetime(date))
        dt = TimeDelta(exposure, format='sec')
        t_end = t_start + dt
        t_start.format = 'mjd'
        t_end.format = 'mjd'
        mjd_start = t_start.value
        mjd_end = t_end.value
        return mjd_start, mjd_end
    return None, None


# TODO - this can be moved to 'the common library code'
def _to_float(value):
    return float(value) if value is not None else None


# TODO - this can be moved to 'the common library code'
def _get_datetime(from_value):
    """
    Ensure datetime values are in MJD.
    :param from_value:
    :return:
    """

    if from_value:
        try:
            return datetime.strptime(from_value, '%Y-%m-%dT%H:%M:%S')
        except ValueError:
            try:
                return datetime.strptime(from_value,
                                         '%Y-%m-%d %H:%M:%S.%f')
            except ValueError:
                try:
                    return datetime.strptime(from_value, '%Y-%m-%d')
                except ValueError:
                    logging.error(
                        'Cannot parse datetime {}'.format(from_value))
                    return None
    else:
        return None


def _build_blueprints(uri):
    """"""
    # how the one-to-one values between the blueprint and the data are
    # programatically determined
    module = importlib.import_module(__name__)
    blueprint = ObsBlueprint(module=module)
    accumulate_position(blueprint)
    accumulate_obs(blueprint)
    accumulate_plane(blueprint)
    accumulate_artifact(blueprint)
    accumulate_part(blueprint)
    blueprints = {uri: blueprint}
    return blueprints


def main_app():
    args = get_gen_proc_arg_parser().parse_args()
    uri = args.lineage[0].split('/', 1)[1]
    blueprints = _build_blueprints(uri)
    omm_science_file = args.local[0]
    try:
        gen_proc(args, blueprints, omm_science_file=omm_science_file)
    except Exception as e:
        logging.error('Failed caom2gen execution.')
        logging.error(e)
        tb = traceback.format_exc()
        logging.error(tb)
        sys.exit(-1)

    logging.debug('Done omm2caom2 processing.')


def main_app_kwargs(**kwargs):

    # the logic to identify cardinality is here - it's very specific to OMM,
    # and won't be re-used anywhere else that I can currently think of ;)
    # that means defining lineage, uris, observations, product ids is all
    # done here

    fname = kwargs['params']['fname'].replace('.fits', '')
    out_obs_xml = kwargs['params']['out_obs_xml']
    collection = kwargs['params']['collection']
    netrc = kwargs['params']['netrc']

    product_id = fname
    artifact_uri = 'ad:{}/{}.fits.gz'.format(collection, fname)

    omm_science_file = artifact_uri
    try:
        blueprints = _build_blueprints(artifact_uri)
        kwargs['params']['blueprints'] = blueprints
        kwargs['params']['visit_args'] = {'omm_science_file': omm_science_file}
        kwargs['params']['no_validate'] = True
        kwargs['params']['dump_config'] = False
        kwargs['params']['ignore_partial_wcs'] = True
        kwargs['params']['plugin'] = __name__
        kwargs['params']['out_obs_xml'] = out_obs_xml
        kwargs['params']['observation'] = product_id
        kwargs['params']['product_id'] = product_id
        kwargs['params']['uri'] = artifact_uri
        kwargs['params']['netrc'] = netrc
        gen_proc_no_args(**kwargs)
    except Exception as e:
        logging.error('Failed caom2gen execution.')
        logging.error(e)
        tb = traceback.format_exc()
        logging.error(tb)
        sys.exit(-1)

    logging.debug('modified Done omm2caom2 processing.')
