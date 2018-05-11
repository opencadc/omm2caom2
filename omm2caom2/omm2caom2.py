import logging
import os
import sys
import traceback

from datetime import datetime

from caom2 import TargetType, ObservationIntentType, CalibrationLevel
from caom2 import DataProductType, ProductType, Observation, Chunk
from caom2 import CoordRange1D, RefCoord, CoordPolygon2D, ValueCoord2D
from caom2 import CoordFunction1D, CoordAxis1D, Axis, TemporalWCS
from caom2utils import ObsBlueprint, get_gen_proc_arg_parser, gen_proc
from caom2utils import gen_proc_no_args

from astropy.time import Time, TimeDelta

from . import footprintfinder

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
    logging.debug('Begin accumulate_obs.')
    bp.set('Observation.type', 'get_obs_type(header)')
    bp.set('Observation.intent', 'get_obs_intent(header)')
    bp.set_fits_attribute('Observation.instrument.name', ['INSTRUME'])
    bp.set_fits_attribute('Observation.instrument.keywords', ['DETECTOR'])
    bp.set('Observation.instrument.keywords', 'DETECTOR=CPAPIR_HAWAII-2')
    bp.set_fits_attribute('Observation.target.name', ['OBJECT'])
    bp.set('Observation.target.type', TargetType.OBJECT)
    bp.set('Observation.target.standard', False)
    bp.set_fits_attribute('Observation.target_position.point.cval1', ['RA'])
    bp.set_fits_attribute('Observation.target_position.point.cval2', ['DEC'])
    bp.set('Observation.target_position.coordsys', 'ICRS')
    bp.set_fits_attribute('Observation.target_position.equinox', ['EQUINOX'])
    bp.set_default('Observation.target_position.equinox', '2000')
    bp.set_fits_attribute('Observation.telescope.name', ['TELESCOP'])
    bp.set_fits_attribute('Observation.telescope.geoLocationX', ['OBS_LAT'])
    bp.set_fits_attribute('Observation.telescope.geoLocationY', ['OBS_LON'])
    bp.set('Observation.telescope.geoLocationZ', 'get_telescope_z(header)')
    bp.set_fits_attribute('Observation.telescope.keywords', ['OBSERVER'])
    bp.set_default('Observation.telescope.keywords', 'UNKNOW')
    bp.set('Observation.environment.ambientTemp',
           'get_obs_env_ambient_temp(header)')


def accumulate_plane(bp):
    logging.debug('Begin accumulate_plane.')
    bp.set('Plane.dataProductType', 'image')
    bp.set('Plane.calibrationLevel', 'get_plane_cal_level(header)')
    bp.set_fits_attribute('Plane.provenance.name', ['INSTRUME'])
    bp.set_fits_attribute('Plane.provenance.runID', ['NIGHTID'])
    bp.set_fits_attribute('Plane.metaRelease', ['DATE-OBS'])
    bp.set('Plane.provenance.version', '1.0')
    # TODO not in the model bp.set('Plane.provenance.product', 'Artigau')
    # TODO set in defaults
    # bp.set_fits_attribute('Plane.provenance.producer', ['ORIGIN'])
    bp.set('Plane.provenance.reference', 'http://genesis.astro.umontreal.ca')
    bp.set('Plane.provenance.project', 'Standard Pipeline')


def accumulate_artifact(bp):
    logging.debug('Begin accumulate_artifact.')
    bp.set('Artifact.productType', 'get_artifact_product_type(uri)')


def accumulate_part(bp):
    logging.debug('Begin accumulate part.')
    bp.set('Part.productType', 'get_part_product_type(header)')


def accumulate_chunk(bp):
    logging.debug('Begin accumulate chunk.')
    bp.set('Chunk.naxis', '4')
    bp.set('Chunk.time.resolution', '0.1')
    # TODO - waiting for an answer from Daniel on what value matters here
    # bp.set('Chunk.position.equinox', 'None')


# def accumulate_time(bp):
#     logging.debug('Begin accumulate_time.')
#     bp.configure_time_axis(4)
#     bp.set_fits_attribute('Chunk.time.exposure', ['TEXP'])
#     bp.set('Chunk.time.timesys', 'UTC')
#     bp.set('Chunk.time.trefpos', 'TOPOCENTER')
#     bp.set('Chunk.time.axis.axis.ctype', 'TIME')
#     bp.set('Chunk.time.axis.axis.cunit', 'd')
#     bp.set('Chunk.time.axis.function.naxis', 4)
#     bp.set('Chunk.time.axis.range.start.pix', 0.5)
#     bp.set_fits_attribute('Chunk.time.axis.range.start.val', ['MJD_STAR'])
#     bp.set('Chunk.time.axis.range.end.pix', 1.5)
#     bp.set_fits_attribute('Chunk.time.axis.range.end.val', ['MJD_END'])


def accumulate_energy(bp):
    logging.debug('Begin accumulate_energy.')
    bp.configure_energy_axis(3)
    bp.set('Chunk.energy.specsys', 'TOPCENT')
    bp.set('Chunk.energy.ssysobs', 'TOPCENT')
    bp.set('Chunk.energy.ssyssrc', 'TOPCENT')
    bp.set_fits_attribute('Chunk.energy.bandpassName', ['FILTER'])
    bp.set('Chunk.energy.axis.axis.ctype', 'WAVE')
    bp.set('Chunk.energy.axis.axis.cunit', 'um')
    bp.set('Chunk.energy.axis.function.naxis', 3)
    bp.set('Chunk.energy.axis.range.start.pix', 0.5)
    bp.set('Chunk.energy.axis.range.start.val',
           'get_start_ref_coord_val(header)')
    bp.set('Chunk.energy.axis.range.end.pix', 1.5)
    bp.set('Chunk.energy.axis.range.end.val', 'get_end_ref_coord_val(header)')


def accumulate_position(bp):
    logging.debug('Begin accumulate_position.')
    bp.configure_position_axes((1, 2))
    bp.set('Chunk.position.coordsys', 'ICRS')
    bp.set('Chunk.position.resolution', 'get_position_resolution(header)')
    bp.set('Chunk.position.axis.axis1.ctype', 'RA---TAN')
    bp.set('Chunk.position.axis.axis1.cunit', 'deg')
    bp.set('Chunk.position.axis.axis2.cunit', 'deg')


def get_artifact_product_type(uri):
    logging.debug('uri is {}'.format(uri))
    if '_prev_256' in uri:
        return 'thumbnail'
    elif '_prev' in uri:
        return 'preview'
    else:
        return 'science'


def get_end_ref_coord_val(header):
    wlen = header[0].get('WLEN')
    bandpass = header[0].get('BANDPASS')
    return wlen + bandpass / 2.


def get_obs_type(header):
    obs_type = None
    datatype = header[0].get('DATATYPE')
    if datatype in DATATYPE_LOOKUP:
        obs_type = DATATYPE_LOOKUP[datatype]
    return obs_type


def get_obs_intent(header):
    lookup = ObservationIntentType.CALIBRATION
    datatype = header[0].get('DATATYPE')
    if 'SCIENCE' in datatype or 'REDUC' in datatype:
        lookup = ObservationIntentType.SCIENCE
    return lookup


def get_obs_env_ambient_temp(header):
    lookup = header[0].get('TEMP_WMO')
    if ((isinstance(lookup, float) or isinstance(lookup,
                                                 int)) and lookup < -99.):
        lookup = None
    return lookup


def get_plane_cal_level(header):
    lookup = CalibrationLevel.RAW_STANDARD
    datatype = header[0].get('DATATYPE')
    if 'REDUC' in datatype:
        lookup = CalibrationLevel.CALIBRATED
    return lookup


def get_part_product_type(header):
    lookup = ProductType.CALIBRATION
    datatype = header[0].get('DATATYPE')
    if 'SCIENCE' in datatype or 'REDUC' in datatype:
        lookup = ProductType.SCIENCE
    return lookup


def get_position_resolution(header):
    temp = None
    temp_astr = _to_float(header[0].get('RMSASTR'))
    if temp_astr != -1.0:
        temp = temp_astr
    temp_mass = _to_float(header[0].get('RMS2MASS'))
    if temp_mass != -1.0:
        temp = temp_mass
    return temp


def get_start_ref_coord_val(header):
    wlen = header[0].get('WLEN')
    bandpass = header[0].get('BANDPASS')
    return wlen - bandpass / 2.


def get_telescope_z(header):
    telescope = header[0].get('TELESCOP')
    if 'OMM' in telescope:
        return 1100.
    elif 'CTIO' in telescope:
        return 2200.
    return None


def update(observation, **kwargs):
    logging.debug('Begin update.')

    assert observation, 'non-null observation parameter'
    assert isinstance(observation, Observation), \
        'observation parameter of type Observation'

    for plane in observation.planes:
        for artifact in observation.planes[plane].artifacts:
            for part in observation.planes[plane].artifacts[artifact].parts:
                for chunk in \
                    observation.planes[plane].artifacts[artifact].parts[
                        part].chunks:
                    _update_time(chunk, **kwargs)

    logging.debug('Done update.')


def _update_time(chunk, **kwargs):
    logging.debug('Begin _update_time.')
    assert isinstance(chunk, Chunk), 'Expecting type Chunk'

    # it's acceptable to manufacture time WCS when other WCS axes already
    # exist, because it implies the file wcs was reasonable
    if 'headers' in kwargs and (
            chunk.position is not None or chunk.energy is not None):
        headers = kwargs['headers']
        mjd_start = headers[0].get('MJD_STAR')
        mjd_end = headers[0].get('MJD_END')
        if mjd_start is None or mjd_end is None:
            mjd_start, mjd_end = _convert_time(headers)
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
    # how the one-to-one values between the blueprint and the data are
    # programatically determined
    module = importlib.import_module(__name__)
    blueprint = ObsBlueprint(module=module)

    # configure the main blueprint
    # accumulate_time(blueprint) - done later ...
    accumulate_energy(blueprint)
    accumulate_position(blueprint)
    accumulate_obs(blueprint)
    accumulate_plane(blueprint)
    accumulate_artifact(blueprint)
    accumulate_part(blueprint)
    accumulate_chunk(blueprint)

    # manage multiple blueprints - one for the fits file, one for the preview,
    # and one for the thumbnail
    blueprints = {}
    blueprints[uri] = blueprint
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

    logging.error(kwargs['params'])
    fname = kwargs['params']['fname'].replace('.fits', '')
    out_obs_xml = kwargs['params']['out_obs_xml']
    collection = kwargs['params']['collection']
    netrc = kwargs['params']['netrc']

    product_id = fname
    artifact_uri = 'ad:{}/{}.fits.gz'.format(collection, fname)

    omm_science_file = artifact_uri

    blueprints = _build_blueprints(artifact_uri)
    kwargs['params']['blueprints'] = blueprints
    kwargs['params']['omm_science_file'] = omm_science_file
    kwargs['params']['no_validate'] = True
    kwargs['params']['dump_config'] = False
    kwargs['params']['ignore_partial_wcs'] = True
    kwargs['params']['plugin'] = ''
    kwargs['params']['out_obs_xml'] = out_obs_xml
    kwargs['params']['observation'] = product_id
    kwargs['params']['product_id'] = product_id
    kwargs['params']['uri'] = artifact_uri
    kwargs['params']['netrc'] = netrc
    try:
        gen_proc_no_args(**kwargs)
    except Exception as e:
        logging.error('Failed caom2gen execution.')
        logging.error(e)
        tb = traceback.format_exc()
        logging.error(tb)
        sys.exit(-1)

    logging.debug('modified Done omm2caom2 processing.')
