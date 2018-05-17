import logging
import os

from caom2 import Observation, CoordPolygon2D, ValueCoord2D, Chunk
from omm2caom2 import footprintfinder

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
        coords = stc.split('Polygon FK5')[1].split()
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

