import logging
import os

from caom2 import Observation, CoordPolygon2D, ValueCoord2D, Chunk
from omm2caom2 import footprintfinder, manage_composable


__all__ = ['visit']


def visit(observation, **kwargs):
    assert observation is not None, 'Input parameter must have a value.'
    assert isinstance(observation, Observation), \
        'Input parameter must be an Observation'

    working_dir = './'
    if 'working_directory' in kwargs:
        working_dir = kwargs['working_directory']
    science_file = None
    if 'science_file' in kwargs:
        science_file = kwargs['science_file']
    science_fqn = os.path.join(working_dir, science_file)
    # TODO - this moves location handling structures to other than the
    # main composable code - this could be MUCH better handled, just not
    # sure how right now
    log_file_directory = None
    if 'log_file_directory' in kwargs:
        log_file_directory = kwargs['log_file_directory']

    count = 0
    for i in observation.planes:
        plane = observation.planes[i]
        for j in plane.artifacts:
            artifact = plane.artifacts[j]
            for k in artifact.parts:
                part = artifact.parts[k]
                for chunk in part.chunks:
                    _update_position(chunk, science_fqn)
                    count += 1

    return_file = '{}_footprint.txt'.format(observation.observation_id)
    return_string_file = '{}_footprint_returnstring.txt'.format(
        observation.observation_id)
    _handle_footprint_logs(log_file_directory, return_file)
    _handle_footprint_logs(log_file_directory, return_string_file)
    return {'chunks': count}


def _update_position(chunk, science_fqn):
    """This function assumes that if the code got here, the science file is
    on disk."""
    logging.debug('Begin _update_position')
    assert isinstance(chunk, Chunk), 'Expecting type Chunk'

    if (chunk.position is not None
            and chunk.position.axis is not None):
        logging.debug('position exists, calculate footprints for {}.'.format(
            science_fqn))
        full_area, footprint_xc, footprint_yc, ra_bary, dec_bary, \
            footprintstring, stc = footprintfinder.main(
                '-r -f  {}'.format(science_fqn))
        logging.debug('footprintfinder result: full area {} '
                      'footprint xc {} footprint yc {} ra bary {} '
                      'dec_bary {} footprintstring {} stc {}'.format(
                        full_area, footprint_xc, footprint_yc, ra_bary,
                        dec_bary, footprintstring, stc))
        bounds = CoordPolygon2D()
        coords = None
        fp_results = stc.split('Polygon FK5')
        if len(fp_results) > 1:
            coords = fp_results[1].split()
        else:
            fp_results = stc.split('Polygon ICRS')
            if len(fp_results) > 1:
                coords = fp_results[1].split()

        if coords is None:
            raise manage_composable.CadcException(
                'Do not recognize footprint {}'.format(stc))

        index = 0
        while index < len(coords):
            vertex = ValueCoord2D(_to_float(coords[index]),
                                  _to_float(coords[index + 1]))
            bounds.vertices.append(vertex)
            index += 2
            logging.debug('Adding vertex\n{}'.format(vertex))
        chunk.position.axis.bounds = bounds
    logging.debug('Done _update_position.')


# TODO - this can be moved to 'the common library code'
def _to_float(value):
    return float(value) if value is not None else None


def _handle_footprint_logs(log_file_directory, log_file):
    if log_file_directory is not None:
        orig_log_fqn = os.path.join(os.getcwd(), log_file)
        if os.path.exists(orig_log_fqn):
            log_fqn = os.path.join(log_file_directory, log_file)
            os.rename(orig_log_fqn, log_fqn)
            logging.debug('Moving footprint log files from {} to {}'.format(
                orig_log_fqn, log_fqn))
