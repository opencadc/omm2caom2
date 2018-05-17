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
    logging.debug('Begin accumulate_artifact.')
    bp.set('Artifact.productType', 'get_product_type(header)')


def accumulate_part(bp):
    logging.debug('Begin accumulate part.')
    bp.set('Part.productType', 'get_product_type(header)')


def accumulate_position(bp):
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


def get_product_type(header):
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
    logging.debug('Begin _update_energy')
    assert isinstance(chunk, Chunk), 'Expecting type Chunk'
    wlen = headers[0].get('WLEN')
    bandpass = headers[0].get('BANDPASS')
    if wlen < 0 or bandpass < 0:
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
    logging.debug('Begin _update_time.')
    assert isinstance(chunk, Chunk), 'Expecting type Chunk'

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
