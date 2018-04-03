import logging
#from caom2 import Chunk, TemporalWCS, SpectralWCS, SpatialWCS
from caom2 import TargetType, ObservationIntentType, CalibrationLevel
from caom2 import ProductType
from caom2utils import fits2caom2, ObsBlueprint


def accumulate_obs(bp):
    logging.error('accumulate_obs')
    # 'Observation.observationID',
    bp.set('Observation.type', '_get_obs_type(header)')
    bp.set('Observation.intent', '_get_obs_intent(header)')
    # 'Observation.sequenceNumber',
    # 'Observation.metaRelease',
    # 'Observation.requirements.flag',
    #
    # 'Observation.algorithm.name',
    #
    bp.set_fits_attribute('Observation.instrument.name', ['INSTRUME'])
    bp.set_fits_attribute('Observation.instrument.keywords', ['DETECTOR'])
    bp.set_default('Observation.instrument.keywords', 'CPAPIR-HAWAII-2')
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
    bp.set_fits_attribute('Observation.telescope.geoLocationY', ['OBS_LONG'])
    bp.set('Observation.telescope.geoLocationZ', '_get_telescope_z(header)')
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
    bp.set('Plane.dataProductType', '_get_plane_product_type(header)')
    bp.set('Plane.calibrationLevel', '_get_plane_cal_level(header,')
    pass

def accumulate_time(bp):
    logging.error('accumulate_time')
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
    # 'Chunk.time.axis.function.naxis',
    # 'Chunk.time.axis.function.delta',
    # 'Chunk.time.axis.function.refCoord.pix',
    # 'Chunk.time.axis.function.refCoord.val',
    bp.set('Chunk.time.axis.range.start.pix', 0.5)
    bp.set_fits_attribute('Chunk.time.axis.range.start.val', ['MJD_STAR'])
    bp.set('Chunk.time.axis.range.end.pix', 1.5)
    bp.set('Chunk.time.axis.range.end.val', ['MJD_END'])
    # return TemporalWCS()


def accumulate_energy(bp):
    logging.error('accumulate_energy')
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
    # 'Chunk.energy.axis.function.naxis',
    # 'Chunk.energy.axis.function.delta',
    # bp.set('Chunk.energy.axis.function.refCoord.pix', 0.5)
    # bp.set('Chunk.energy.axis.function.refCoord.val',
    #        '_getLowerRefCoordVal(header)')
    bp.set('Chunk.energy.axis.range.start.pix', 0.5)
    bp.set('Chunk.energy.axis.range.start.val', '_get_start_ref_coord_val(header)')
    bp.set('Chunk.energy.axis.range.end.pix', 1.5)
    bp.set('Chunk.energy.axis.range.end.val', '_get_end_ref_coord_val(header)')

    # return SpectralWCS()


def accumulate_position(bp):
    logging.error('accumulate_position')
    bp.configure_position_axes((1, 2))
    bp.set('Chunk.position.coordsys', 'ICRS')
    # 'Chunk.position.equinox',
    bp.set('Chunk.position.resolution', '_get_position_resolution(header)')
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


def _get_end_ref_coord_val(header):
    wlen = header[0].get('WLEN')
    bandpass = header[0].get('BANDPASS')
    return wlen + bandpass / 2.


def _get_obs_type(header):
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


def _get_obs_intent(header):
    lookup = ObservationIntentType.CALIBRATION
    datatype = header[0].get('DATATYPE')
    if datatype.find('SCIENCE') != -1 or datatype.find('REDUC') != -1:
        lookup = ObservationIntentType.SCIENCE
    return lookup


def _get_plane_cal_level(header):
    lookup = CalibrationLevel.RAW_STANDARD
    datatype = header[0].get('DATATYPE')
    if datatype.find('REDUC') != -1:
        lookup = CalibrationLevel.CALIBRATED
    return lookup


def _get_plane_product_type(header):
    lookup = ProductType.CALIBRATION
    datatype = header[0].get('DATATYPE')
    if datatype.find('SCIENCE') != -1 or datatype.find('REDUC') != -1:
        lookup = ProductType.SCIENCE
    return lookup


def _get_position_resolution(header):
    temp = None
    temp_astr = fits2caom2._to_float(header[0].get('RMSASTR'))
    if temp_astr != -1.0:
        temp = temp_astr
    temp_mass = fits2caom2._to_float(header[0].get('RMS2MASS'))
    if temp_mass != -1.0:
        temp = temp_mass
    return temp


def _get_start_ref_coord_val(header):
    wlen = header[0].get('WLEN')
    bandpass = header[0].get('BANDPASS')
    return wlen - bandpass / 2.


def _get_telescope_z(header):
    telescope = header[0].get('TELESCOP')
    if telescope.find('OMM') != -1:
        return 1100.
    elif telescope.find('CTIO'):
        return 2200.
    return None


def main_app():
    blueprint = ObsBlueprint()
    accumulate_time(blueprint)
    accumulate_energy(blueprint)
    accumulate_position(blueprint)
    accumulate_obs(blueprint)
    # chunk = Chunk()
    # chunk.time = accumulate_time()
    # chunk.energy = accumulate_energy()
    # chunk.position = accumulate_position()
