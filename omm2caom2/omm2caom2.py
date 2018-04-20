import logging
import os
import sys
import traceback

from datetime import datetime

#from caom2 import Chunk, TemporalWCS, SpectralWCS, SpatialWCS
from caom2 import TargetType, ObservationIntentType, CalibrationLevel
from caom2 import DataProductType, Observation, Chunk
from caom2 import CoordRange1D, RefCoord, CoordPolygon2D, ValueCoord2D
from caom2utils import ObsBlueprint, get_gen_proc_arg_parser, gen_proc

from astropy.time import Time, TimeDelta

from . import footprintfinder

import importlib


def accumulate_obs(bp):
    logging.debug('Begin accumulate_obs.')
    # 'Observation.observationID',
    bp.set('Observation.type', 'get_obs_type(header)')
    bp.set('Observation.intent', 'get_obs_intent(header)')
    # 'Observation.sequenceNumber',
    # 'Observation.metaRelease',
    # 'Observation.requirements.flag',
    #
    # 'Observation.algorithm.name',
    #
    bp.set_fits_attribute('Observation.instrument.name', ['INSTRUME'])
    bp.set_fits_attribute('Observation.instrument.keywords', ['DETECTOR'])
    bp.set('Observation.instrument.keywords', 'DETECTOR=CPAPIR_HAWAII-2')
    #
    # 'Observation.proposal.id',
    # 'Observation.proposal.pi',
    # 'Observation.proposal.project',
    # 'Observation.proposal.title',
    # 'Observation.proposal.keywords',
    #
    bp.set_fits_attribute('Observation.target.name', ['OBJECT'])
    bp.set('Observation.target.type', TargetType.OBJECT)
    bp.set('Observation.target.standard', False)
    # 'Observation.target.redshift',
    # 'Observation.target.keywords',
    # 'Observation.target.moving',

    bp.set_fits_attribute('Observation.target_position.point.cval1', ['RA'])
    bp.set_fits_attribute('Observation.target_position.point.cval2', ['DEC'])
    bp.set('Observation.target_position.coordsys', 'ICRS')
    bp.set_fits_attribute('Observation.target_position.equinox', ['EQUINOX'])
    bp.set_default('Observation.target_position.equinox', '2000')
    #
    bp.set_fits_attribute('Observation.telescope.name', ['TELESCOP'])
    bp.set_fits_attribute('Observation.telescope.geoLocationX', ['OBS_LAT'])
    bp.set_fits_attribute('Observation.telescope.geoLocationY', ['OBS_LON'])
    bp.set('Observation.telescope.geoLocationZ', 'get_telescope_z(header)')
    bp.set_fits_attribute('Observation.telescope.keywords', ['OBSERVER'])
    bp.set_default('Observation.telescope.keywords', 'UNKNOW')
    #
    # 'Observation.environment.seeing',
    # 'Observation.environment.humidity',
    # 'Observation.environment.elevation',
    # 'Observation.environment.tau',
    # 'Observation.environment.wavelengthTau',
    bp.set_fits_attribute('Observation.environment.ambientTemp', ['TEMP_WMO'])
    # 'Observation.environment.photometric',


def accumulate_plane(bp):
    logging.debug('Begin accumulate_plane.')
    bp.set('Plane.dataProductType', 'get_plane_data_product_type(header)')
    bp.set('Plane.calibrationLevel', 'get_plane_cal_level(header)')
    bp.set_fits_attribute('Plane.provenance.name', ['INSTRUME'])
    bp.set_fits_attribute('Plane.provenance.runID', ['NIGHTID'])
    bp.set('Plane.provenance.version', '1.0')
    # TODO not in the model bp.set('Plane.provenance.product', 'Artigau')
    # TODO set in defaults
    # bp.set_fits_attribute('Plane.provenance.producer', ['ORIGIN'])
    bp.set('Plane.provenance.reference', 'http://genesis.astro.umontreal.ca')
    bp.set('Plane.provenance.project', 'Standard Pipeline')


def accumulate_time(bp):
    logging.debug('Begin accumulate_time.')
    bp.configure_time_axis(4)
    bp.set_fits_attribute('Chunk.time.exposure', ['TEXP'])
    # 'Chunk.time.resolution',
    bp.set('Chunk.time.timesys', 'UTC')
    bp.set('Chunk.time.trefpos', 'TOPOCENTER')
    # 'Chunk.time.mjdref',
    bp.set('Chunk.time.axis.axis.ctype', 'TIME')
    bp.set('Chunk.time.axis.axis.cunit', 'd')
    # 'Chunk.time.axis.bounds.samples',
    # 'Chunk.time.axis.error.syser',
    # 'Chunk.time.axis.error.rnder',
    bp.set('Chunk.time.axis.function.naxis', 4)
    # 'Chunk.time.axis.function.delta',
    # 'Chunk.time.axis.function.refCoord.pix',
    # 'Chunk.time.axis.function.refCoord.val',
    bp.set('Chunk.time.axis.range.start.pix', 0.5)
    bp.set_fits_attribute('Chunk.time.axis.range.start.val', ['MJD_STAR'])
    # bp.set('Chunk.time.axis.range.start.val', 'get_mjd_star(header)')
    bp.set('Chunk.time.axis.range.end.pix', 1.5)
    bp.set_fits_attribute('Chunk.time.axis.range.end.val', ['MJD_END'])
    # bp.set('Chunk.time.axis.range.end.val', 'get_mjd_end(header)')


def accumulate_energy(bp):
    logging.debug('Begin accumulate_energy.')
    bp.configure_energy_axis(3)
    bp.set('Chunk.energy.specsys', 'TOPOCENT')
    bp.set('Chunk.energy.ssysobs', 'TOPOCENT')
    bp.set('Chunk.energy.ssyssrc', 'TOPOCENT')

    # 'Chunk.energy.restfrq',
    # 'Chunk.energy.restwav',
    # 'Chunk.energy.velosys',
    # 'Chunk.energy.zsource',
    # 'Chunk.energy.velang',

    bp.set_fits_attribute('Chunk.energy.bandpassName', ['FILTER'])

    # 'Chunk.energy.resolvingPower',
    # 'Chunk.energy.transition',
    # 'Chunk.energy.transition.species',
    # 'Chunk.energy.transition.transition',

    bp.set('Chunk.energy.axis.axis.ctype', 'WAVE')
    bp.set('Chunk.energy.axis.axis.cunit', 'um')

    # 'Chunk.energy.axis.bounds.samples',
    # 'Chunk.energy.axis.error.syser',
    # 'Chunk.energy.axis.error.rnder',
    bp.set('Chunk.energy.axis.function.naxis', 3)
    # 'Chunk.energy.axis.function.delta',
    # bp.set('Chunk.energy.axis.function.refCoord.pix', 0.5)
    # bp.set('Chunk.energy.axis.function.refCoord.val',
    #        'getLowerRefCoordVal(header)')
    bp.set('Chunk.energy.axis.range.start.pix', 0.5)
    bp.set('Chunk.energy.axis.range.start.val',
           'get_start_ref_coord_val(header)')
    bp.set('Chunk.energy.axis.range.end.pix', 1.5)
    bp.set('Chunk.energy.axis.range.end.val', 'get_end_ref_coord_val(header)')


def accumulate_position(bp):
    logging.debug('Begin accumulate_position.')
    bp.configure_position_axes((1, 2))
    bp.set('Chunk.position.coordsys', 'ICRS')
    # 'Chunk.position.equinox',
    bp.set('Chunk.position.resolution', 'get_position_resolution(header)')
    bp.set('Chunk.position.axis.axis1.ctype', 'RA---TAN')
    bp.set('Chunk.position.axis.axis1.cunit', 'deg')
    # 'Chunk.position.axis.axis2.ctype',
    bp.set('Chunk.position.axis.axis2.cunit', 'deg')
    # 'Chunk.position.axis.error1.syser',
    # 'Chunk.position.axis.error1.rnder',
    # 'Chunk.position.axis.error2.syser',
    # 'Chunk.position.axis.error2.rnder',
    # bp.set_fits_attribute('Chunk.position.axis.function.cd11', ['CD1_1'])
    # bp.set_fits_attribute('Chunk.position.axis.function.cd12', ['CD1_2'])
    # bp.set_fits_attribute('Chunk.position.axis.function.cd21', ['CD2_1'])
    # bp.set_fits_attribute('Chunk.position.axis.function.cd22', ['CD2_2'])
    # 'Chunk.position.axis.function.dimension.naxis1',
    # 'Chunk.position.axis.function.dimension.naxis2',
    # bp.set_fits_attribute('Chunk.position.axis.function.refCoord.coord1.pix', ['CRPIX1'])
    # bp.set_fits_attribute('Chunk.position.axis.function.refCoord.coord1.val', ['CRVAL1'])
    # bp.set_fits_attribute('Chunk.position.axis.function.refCoord.coord2.pix', ['CRPIX2'])
    # bp.set_fits_attribute('Chunk.position.axis.function.refCoord.coord2.val', ['CRVAL2'])
    # 'Chunk.position.axis.range.start.coord1.pix',
    # 'Chunk.position.axis.range.start.coord1.val',
    # 'Chunk.position.axis.range.start.coord2.pix',
    # 'Chunk.position.axis.range.start.coord2.val',
    # 'Chunk.position.axis.range.end.coord1.pix',
    # 'Chunk.position.axis.range.end.coord1.val',
    # 'Chunk.position.axis.range.end.coord2.pix',
    # 'Chunk.position.axis.range.end.coord2.val',
    # return SpatialWCS()


def get_end_ref_coord_val(header):
    wlen = header[0].get('WLEN')
    bandpass = header[0].get('BANDPASS')
    return wlen + bandpass / 2.


def get_obs_type(header):
    obs_type = None
    datatype = header[0].get('DATATYPE')
    DATATYPE_LOOKUP = {'CALIB': 'flat',
                       'SCIENCE': 'object',
                       'FOCUS': 'focus',
                       'REDUC': 'reduc',
                       'TEST': 'test',
                       'REJECT': 'reject',
                       'CALRED': 'flat'}
    if datatype in DATATYPE_LOOKUP:
        obs_type = DATATYPE_LOOKUP[datatype]
    return obs_type


def get_obs_intent(header):
    lookup = ObservationIntentType.CALIBRATION
    datatype = header[0].get('DATATYPE')
    if datatype.find('SCIENCE') != -1 or datatype.find('REDUC') != -1:
        lookup = ObservationIntentType.SCIENCE
    return lookup


def get_plane_cal_level(header):
    lookup = CalibrationLevel.RAW_STANDARD
    datatype = header[0].get('DATATYPE')
    if datatype.find('REDUC') != -1:
        lookup = CalibrationLevel.CALIBRATED
    return lookup


def get_plane_data_product_type(header):
    # TODO - what I copied from Daniel is not consistent with the
    # set of possible values for DataProductType.
    # Daniel to provide more information.
    lookup = DataProductType.IMAGE
    datatype = header[0].get('DATATYPE')
    if datatype.find('SCIENCE') != -1 or datatype.find('REDUC') != -1:
        lookup = DataProductType.CUBE
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
    if telescope.find('OMM') != -1:
        return 1100.
    elif telescope.find('CTIO'):
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
                    _update_position(chunk, **kwargs)
                    _update_time(chunk, **kwargs)

    logging.debug('Done update.')


def _update_position(chunk, **kwargs):
    logging.debug('Begin _update_position')
    assert isinstance(chunk, Chunk), 'Expecting type Chunk'

    if ('omm_science_file' in kwargs and chunk.position is not None
            and chunk.position.axis is not None):
        logging.debug('position exists, calculate footprints.')
        oms = kwargs['omm_science_file'].replace('.header', '')
        full_area, footprint_xc, footprint_yc, ra_bary, dec_bary, \
            footprintstring, stc = footprintfinder.main(
                '-r -f  {}'.format(oms))
        logging.debug('footprintfinder result: full area {} '
                      'footprint xc {} footprint yc {} ra bary {} '
                      'dec_bary {} footprintstring {} stc {}'.format(
                        full_area, footprint_xc, footprint_yc, ra_bary,
                        dec_bary, footprintstring, stc))
        bounds = CoordPolygon2D()
        coords = footprintstring.split(',')
        index = 0
        while index < len(coords):
            vertex = ValueCoord2D(_to_float(coords[index]),
                                  _to_float(coords[index + 1]))
            bounds.vertices.append(vertex)
            index += 2
            logging.debug('Adding vertex\n{}'.format(vertex))
        chunk.position.axis.bounds = bounds
    logging.debug('Done _update_position.')


def _update_time(chunk, **kwargs):
    logging.debug('Begin _update_time.')
    assert isinstance(chunk, Chunk), 'Expecting type Chunk'

    if ('headers' in kwargs and chunk.time is not None
            and chunk.time.axis is not None):
        logging.debug('time axis exists, calculate range.')
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
        chunk.time.axis.range = CoordRange1D(start, end)
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


def main_app():
    args = get_gen_proc_arg_parser().parse_args()

    # how the one-to-one values between the blueprint and the data are
    # programatically determined
    module = importlib.import_module(__name__)
    blueprint = ObsBlueprint(module=module)
    accumulate_time(blueprint)
    accumulate_energy(blueprint)
    accumulate_position(blueprint)
    accumulate_obs(blueprint)
    accumulate_plane(blueprint)

    blueprints = {'1': blueprint}

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
