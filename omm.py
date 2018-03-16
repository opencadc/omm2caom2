from __future__ import (absolute_import, division, print_function,
                      unicode_literals)

from cadcutils import util
import sys
from caom2utils import fits2caom2
from caom2 import SimpleObservation
import argparse

#
# from caom2repo import core
#
# if __name__ == “__main__“:
#     sys.argv = (‘caom2-repo visit --plugin ’
#                 ‘/Users/adriand/work/github/caom2tools/caom2repo/caom2repo’
#                 ‘/tests/artifacturiplugin.py -v --cert /Users/adriand/.ssl’
#                 ‘/cadcproxy.pem --threads 3  --obs_file /Users/adriand/work/github/caom2tools/caom2repo/test.in CFHTMEGAPIPE’).split()
#     #sys.argv = ‘caom2-repo read -d -n GEMINI GN-CAL20080312-4-007’.split()
#     core.main_app()



# curate images, cadc-data put - they're worried about losing files, so as soon
# possible they will be making a copy

"""
To run that 
- plan to optimize downloads
- works on one file at a time
- if the file is local, it will still fetch the info of the file from the
collection here, compute the checksum, make sure they match before working with
the local copy
- if the file doesn't exist - retrieve the file from ad - need the pixels to
be able to compute a footprint
- if the checksums don't match - error and stop


"""

sys.argv = ‘omm-ingest /tmp/c18.fits’.split()
parser = util.get_base_parser(subparsers=False, version=‘1.0’,
                            default_resource_id=‘ivo://cadc.nrc.ca/ommInges’)
parser.add_argument(‘fits’, help=‘Source FITS file for the observation’)
parser.add_argument(‘--test’, action=‘store_true’,
                  help=‘test mode, do not persist to database’)
args = parser.parse_args()
# find local file
# find remote file and compare with local (if exists)
# download remote if not exists
# error if remote different from local

# do the preview and save them
# do the footprint


"""
This is not what people should have to work with.
- need two or three functions to do the following:
- time - starting date and exposure time - compute the necessary information, and
fill the caom2 structure
- energy - central wavelength and the bandpass (filter size) - compute the
necessary information, and fill the caom2 structure


Hide the caom2 structure from OMM.

Everything will be Simple Observations for the moment.
- FITS knowledgeable
- 
"""

bp = fits2caom2.ObsBlueprint(position_axes=(1, 2), energy_axis=3, time_axis=4)
bp.set_fits_attribute(‘Plane.metaRelease’, [‘DATE-OBS’])
bp.set_fits_attribute(‘Plane.dataRelease’, [‘RELEASE’])
bp.set(‘Plane.provenance.version’, ‘1.0’)
bp.set_fits_attribute(‘Plane.provenance.runID’, [‘NIGHTID’])
bp.set_fits_attribute(‘Plane.provenance.producer’, [‘ORIGIN’])
bp.set(‘Artifact.productType’, ‘science’)
bp.set(‘Artifact.releaseType’, ‘data’)
bp.set(‘Plane.dataProductType’, ‘image’)
bp.set(‘Plane.calibrationLevel’, 1)
bp.set_fits_attribute(‘Chunk.time.exposure’, [‘TEXP’])
bp.set(‘Chunk.time.axis.axis.ctype’, ‘TIME’)
bp.set(‘Chunk.time.axis.axis.cunit’, ‘d’)

bp.set(‘Chunk.energy.axis.axis.ctype’, ‘WAVE’)
bp.set(‘Chunk.energy.axis.axis.cunit’, ‘um’)

print(bp)

fits_parser = fits2caom2.FitsParser(args.fits, obs_blueprint=bp)
obs = SimpleObservation(‘OMM’, ‘test’)
fits_parser.augment_observation(obs, ‘ad:omm/c18.fits’, ‘myProdID’)
print(obs)


#print(args)
