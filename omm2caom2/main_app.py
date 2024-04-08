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

"""
DD - slack - 24-03-20
Once deployed, @Sharon Goliath will start, eventually, a task at CADC which
will: 1- identify al SCI observations without RA/DEC, i.e. valid WCS and will
modify the archive catalog for these observations to be junk. This means they
will disappear from the regular catalog. These junked observations will be
available only through the programmatic TAP interface or using topcat pointing
to https://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/argus/

"""

import numpy
import os
import re

from astropy.io import fits
import astropy.wcs as wcs

from caom2 import (
    TargetType, ObservationIntentType, CalibrationLevel,
    ProductType, Observation, Chunk, CoordRange1D, RefCoord,
    CoordFunction1D, CoordAxis1D, Axis, TemporalWCS, SpectralWCS,
    ObservationURI, PlaneURI, TypedSet, CoordBounds1D, Quality,
    Requirements, Status, Instrument, Provenance, DataQuality,
    SimpleObservation, DerivedObservation, Algorithm,
)
from caom2utils.caom2blueprint import update_artifact_meta
from caom2pipe import astro_composable as ac
from caom2pipe.caom_composable import change_to_composite, TelescopeMapping
from caom2pipe.manage_composable import (
    CadcException,
    CaomName,
    check_param,
    make_datetime,
    StorageName,
    to_float,
    update_typed_set,
)
from caom2pipe import name_builder_composable as nbc


__all__ = ['OmmName', 'OmmBuilder', 'Telescope']


# map the fits file values to the DataProductType enums
DATATYPE_LOOKUP = {
    'CALIB': 'flat',
    'SCIENCE': 'object',
    'FOCUS': 'focus',
    'REDUC': 'reduc',
    'TEST': 'test',
    'REJECT': 'reject',
    'CALRED': 'flat',
}

DEFAULT_GEOCENTRIC = {
    'OMM': {
        'x': 1448045.773,
        'y': -4242075.462,
        'z': 4523808.146,
        'elevation': 1108.0,
    },
    'CTIO': {
        'x': 1814303.745,
        'y': -5214365.744,
        'z': -3187340.566,
        'elevation': 2200.0,
    },
}


class OmmBuilder(nbc.StorageNameBuilder):
    def __init__(self, config):
        self._config = config

    def build(self, entry):
        """
        OMM is the original pipeline, don't want to break any of the behaviour, so set up the source names here.

        :param entry: str - from an os.listdir call, so will have no notion of 'source'
        :return:
        """
        temp = os.path.basename(entry)
        if self._config.use_local_files:
            fqn = None
            result = None
            for entry in self._config.data_sources:
                fqn = os.path.join(entry, temp)
                if os.path.exists(fqn):
                    result = OmmName(file_name=temp, source_names=[fqn])
                break
            if result is None:
                raise CadcException(f'Could not find local file {fqn}.')
        else:
            result = OmmName(file_name=temp, source_names=[entry])
        return result


class OmmName(StorageName):
    """OMM naming rules:
    - support mixed-case file name storage, and mixed-case obs id values
    - support gzipped file names in ad, and gzipped and unzipped file names
      otherwise.
    - files are compressed in ad
    """

    OMM_NAME_PATTERN = 'C[\\w+-.]+[SCI|CAL|SCIRED|CALRED|TEST|FOCUS]'
    StorageName.collection_pattern = OMM_NAME_PATTERN

    def __init__(
        self,
        obs_id=None,
        product_id=None,
        file_name=None,
        source_names=[],
    ):
        super().__init__(obs_id, product_id, file_name, source_names)

    @property
    def prev(self):
        """The preview file name for the file."""
        return f'{self._product_id}_prev.jpg'

    @property
    def thumb(self):
        """The thumbnail file name for the file."""
        return f'{self._product_id}_prev_256.jpg'

    def is_rejected(self):
        return '_REJECT' in self._file_id

    def is_valid(self):
        if self._file_id is None:
            self._logger.warning('file_id is not set.')
            result = True
        else:
            pattern = re.compile(self.collection_pattern)
            result = pattern.match(self._file_id)
            if result:
                if '_SCIRED' in self._file_id or '_CALRED' in self._file_id:
                    f_name_id_upper = self._file_id.upper()
                    # file names are mixed case in the middle
                    if (
                        '_DOMEFLAT_' in f_name_id_upper
                        or '_DARK_' in f_name_id_upper
                    ):
                        if (
                            '_domeflat_' in self._file_id
                            or '_dark_' in self._file_id
                        ):
                            result = True
                        else:
                            result = False
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
            return f'{fname}.gz'
        elif fname.endswith('.fits.header'):
            return fname.replace('.header', '.gz')
        elif fname.endswith('.jpg'):
            return fname
        else:
            raise CadcException(f'Unexpected file name {fname}')

    def set_obs_id(self):
        temp = self._file_name.replace('_prev_256.jpg', '').replace(
            '_prev.jpg', ''
        )
        self._obs_id = '_'.join(
            ii for ii in OmmName.remove_extensions(temp).split('_')[:-1]
        )

    @staticmethod
    def is_composite(uri):
        return '_SCIRED' in uri or '_CALRED' in uri

    @staticmethod
    def remove_extensions(f_name):
        return StorageName.remove_extensions(f_name).replace('.jpg', '')


class Telescope(TelescopeMapping):
    def __init__(self, storage_name, headers, clients, observable, observation, config):
        super().__init__(storage_name, headers, clients, observable, observation, config)

    def accumulate_blueprint(self, bp):
        super().accumulate_blueprint(bp)
        self.accumulate_obs(bp)
        self.accumulate_plane(bp)
        self.accumulate_artifact(bp)
        self.accumulate_position(bp)
        self.accumulate_part(bp)

    def accumulate_obs(self, bp):
        """Configure the OMM-specific ObsBlueprint at the CAOM model
        Observation level."""
        self._logger.debug('Begin accumulate_obs.')
        bp.clear('Observation.algorithm.name')
        bp.set('Observation.type', 'get_obs_type()')
        bp.set('Observation.intent', 'get_obs_intent()')
        bp.add_attribute('Observation.instrument.name', 'INSTRUME')
        bp.add_attribute('Observation.instrument.keywords', 'DETECTOR')
        bp.set('Observation.instrument.keywords', 'DETECTOR=CPAPIR-HAWAII-2')
        bp.add_attribute('Observation.target.name', 'OBJECT')
        bp.set('Observation.target.type', TargetType.OBJECT)
        bp.set('Observation.target.standard', False)
        bp.set('Observation.target.moving', False)
        bp.add_attribute('Observation.target_position.point.cval1', 'RA')
        bp.add_attribute(
            'Observation.target_position.point.cval2', 'DEC'
        )
        bp.set('Observation.target_position.coordsys', 'ICRS')
        bp.add_attribute(
            'Observation.target_position.equinox', 'EQUINOX'
        )
        bp.set_default('Observation.target_position.equinox', '2000.0')
        bp.add_attribute('Observation.telescope.name', 'TELESCOP')
        bp.set('Observation.telescope.keywords', 'get_telescope_keywords()')
        bp.set(
            'Observation.environment.ambientTemp',
            'get_obs_env_ambient_temp()',
        )
        if OmmName.is_composite(self._storage_name.file_uri):
            bp.set('CompositeObservation.members', {})

    def accumulate_plane(self, bp):
        """Configure the OMM-specific ObsBlueprint at the CAOM model Plane
        level."""
        self._logger.debug('Begin accumulate_plane.')
        bp.set('Plane.dataProductType', 'image')
        bp.set('Plane.calibrationLevel', 'get_plane_cal_level()')
        bp.add_attribute('Plane.provenance.name', 'INSTRUME')
        bp.add_attribute('Plane.provenance.runID', 'NIGHTID')
        bp.set('Plane.metaRelease', 'get_meta_release_date()')
        bp.set('Plane.dataRelease', 'get_data_release_date()')
        bp.clear('Plane.provenance.version')
        bp.add_attribute('Plane.provenance.version', 'DRS_VERS')
        bp.set_default('Plane.provenance.version', '1.0')
        bp.clear('Plane.provenance.lastExecuted')
        bp.add_attribute('Plane.provenance.lastExecuted', 'DRS_DATE')
        bp.set('Plane.provenance.reference', 'http://omm-astro.ca')
        bp.set('Plane.provenance.project', 'Standard Pipeline')

    def accumulate_artifact(self, bp):
        """Configure the OMM-specific ObsBlueprint at the CAOM model Artifact
        level."""
        self._logger.debug('Begin accumulate_artifact.')
        bp.set('Artifact.productType', 'get_product_type()')
        bp.set('Artifact.releaseType', 'data')

    def accumulate_part(self, bp):
        """Configure the OMM-specific ObsBlueprint at the CAOM model Part
        level."""
        self._logger.debug('Begin accumulate part.')
        bp.set('Part.productType', 'get_product_type()')

    def accumulate_position(self, bp):
        """Configure the OMM-specific ObsBlueprint for the CAOM model
        SpatialWCS."""
        self._logger.debug('Begin accumulate_position.')
        bp.configure_position_axes((1, 2))
        bp.set('Chunk.position.coordsys', 'ICRS')
        bp.set(
            'Chunk.position.axis.error1.rnder',
            'get_position_resolution()',
        )
        bp.set('Chunk.position.axis.error1.syser', 0.0)
        bp.set(
            'Chunk.position.axis.error2.rnder',
            'get_position_resolution()',
        )
        bp.set('Chunk.position.axis.error2.syser', 0.0)
        bp.set('Chunk.position.axis.axis1.ctype', 'RA---TAN')
        bp.set('Chunk.position.axis.axis2.ctype', 'DEC--TAN')
        bp.set('Chunk.position.axis.axis1.cunit', 'deg')
        bp.set('Chunk.position.axis.axis2.cunit', 'deg')
        bp.clear('Chunk.position.equinox')
        bp.add_attribute('Chunk.position.equinox', 'EQUINOX')
        bp.set(
            'Chunk.position.resolution',
            'get_chunk_position_resolution()',
        )

    def get_chunk_position_resolution(self, ext):
        # DD - slack - 11-02-20
        # FWHM meaning is Full width Half Maximum, is a measurement of the
        # stellar profile on the image. So I suspect IQ being that FWHM value.
        return to_float(self._headers[ext].get('FWHM'))

    def get_end_ref_coord_val(self, ext):
        """Calculate the upper bound of the spectral energy coordinate from
        FITS header values.

        Called to fill a blueprint value, must have a
        parameter named ext for import_module loading and execution.
        """
        wlen = self._headers[ext].get('WLEN')
        bandpass = self._headers[ext].get('BANDPASS')
        if wlen is not None and bandpass is not None:
            return wlen + bandpass / 2.0
        else:
            return None

    def get_obs_type(self, ext):
        """Calculate the Observation-level data type from FITS header values.

        Called to fill a blueprint value, must have a
        parameter named ext for import_module loading and execution."""
        obs_type = None
        datatype = self._headers[ext].get('DATATYPE')
        if datatype in DATATYPE_LOOKUP:
            obs_type = DATATYPE_LOOKUP[datatype]
        return obs_type

    def get_obs_intent(self, ext):
        """Calculate the Observation-level intent from FITS header values.

        Called to fill a blueprint value, must have a
        parameter named ext for import_module loading and execution."""
        lookup = ObservationIntentType.CALIBRATION
        datatype = self._headers[ext].get('DATATYPE')
        if 'SCIENCE' in datatype or 'REDUC' in datatype:
            lookup = ObservationIntentType.SCIENCE
        return lookup

    def get_obs_env_ambient_temp(self, ext):
        """Calculate the ambient temperature from FITS header values. Ignore
        what is used for default values, if they exist.

        Called to fill a blueprint value, must have a
        parameter named ext for import_module loading and execution."""
        lookup = self._headers[ext].get('TEMP_WMO')
        if (
            isinstance(lookup, float) or isinstance(lookup, int)
        ) and lookup < -99.0:
            lookup = None
        return lookup

    def get_plane_cal_level(self, ext):
        """Calculate the Plane-level calibration level from FITS header values.

        Called to fill a blueprint value, must have a
        parameter named ext for import_module loading and execution."""
        lookup = CalibrationLevel.RAW_STANDARD
        datatype = self._headers[ext].get('DATATYPE')
        if 'REDUC' in datatype:
            lookup = CalibrationLevel.CALIBRATED
        return lookup

    def get_product_type(self, ext):
        """Calculate the Plane-level, Artifact-level, Part-level, and Chunk-level
         product type from FITS header values.

        Called to fill a blueprint value, must have a
        parameter named ext for import_module loading and execution."""
        lookup = ProductType.CALIBRATION
        datatype = self._headers[ext].get('DATATYPE')
        if 'SCIENCE' in datatype or 'REDUC' in datatype:
            lookup = ProductType.SCIENCE
        return lookup

    def get_position_resolution(self, ext):
        """Calculate the Plane-level position RNDER values from other FITS header
        values. Ignore values used by the telescope as defaults.

        Called to fill a blueprint value, must have a
        parameter named ext for import_module loading and execution."""
        temp = None
        temp_astr = to_float(self._headers[ext].get('RMSASTR'))
        if temp_astr is not None and temp_astr != -1.0:
            temp = temp_astr
        temp_mass = to_float(self._headers[ext].get('RMS2MASS'))
        if temp_mass is not None and temp_mass != -1.0:
            temp = temp_mass
        return temp

    def get_telescope_keywords(self, ext):
        """For Observation.telescope.keywords, ignore values used by the
        telescope as defaults.

        Called to fill a blueprint value, must have a
        parameter named ext for import_module loading and execution."""
        temp = self._headers[ext].get('OBSERVER')
        if temp is not None and 'none' in temp:
            temp = None
        return temp

    def get_data_release_date(self, ext):
        """Use the 'DATE' keyword for the release date, if the 'RELEASE'
        keyword does not exist.

        Called to fill a blueprint value, must have a
        parameter named ext for import_module loading and execution."""
        rel_date = self._headers[ext].get('RELEASE')
        if rel_date is None:
            rel_date = self._headers[ext].get('DATE')
            intent = self.get_obs_intent(ext)
            if (
                rel_date is not None
                and intent is ObservationIntentType.SCIENCE
            ):
                # DD, SB - slack - 19-03-20 - if release is not in the header
                # observation date plus two years. This only applies to
                # science observations.
                temp = make_datetime(rel_date)
                rel_date = temp.replace(year=temp.year + 2)
        return rel_date

    def get_meta_release_date(self, ext):
        """Use the 'DATE' keyword for the release date, if the 'DATE-OBS'
        keyword does not exist.

        Called to fill a blueprint value, must have a
        parameter named ext for import_module loading and execution."""
        rel_date = self._headers[ext].get('DATE-OBS')
        if rel_date is not None:
            return rel_date
        else:
            rel_date = self._headers[ext].get('DATE')
            return rel_date

    def get_start_ref_coord_val(self, ext):
        """Calculate the lower bound of the spectral energy coordinate from
        FITS header values.

        Called to fill a blueprint value, must have a
        parameter named ext for import_module loading and execution."""
        wlen = self._headers[ext].get('WLEN')
        bandpass = self._headers[ext].get('BANDPASS')
        if wlen is not None and bandpass is not None:
            return wlen - bandpass / 2.0
        else:
            return None

    def update(self, file_info):
        """Called to fill multiple CAOM model elements and/or attributes, must
        have this signature for import_module loading and execution.

        :param observation A CAOM Observation model instance.
        :param file_info cadcdata.FileInfo instance
        :param clients ClientCollection instance
        """
        self._logger.debug('Begin update.')
        self._update_telescope_location(self._observation)

        for plane in self._observation.planes.values():
            for artifact in plane.artifacts.values():
                if self._storage_name.file_uri != artifact.uri:
                    continue
                update_artifact_meta(artifact, file_info)
                for part in artifact.parts.values():
                    if len(part.chunks) == 0 and part.name == '0':
                        # always have a time axis, and usually an energy
                        # axis as well, so create a chunk for the zero-th part
                        part.chunks.append(Chunk())

                    for chunk in part.chunks:
                        chunk.product_type = self.get_product_type(0)
                        self._update_energy(chunk)
                        self._update_time(chunk, self._observation.observation_id)
                        self._update_position(plane, self._observation.intent, chunk)
                        if chunk.position is None:
                            # for WCS validation correctness
                            chunk.naxis = None
                            chunk.energy_axis = None
                            chunk.time_axis = None

                if self._storage_name.is_rejected():
                    self._update_requirements(self._observation)

            if OmmName.is_composite(plane.product_id):
                if Telescope.change_type(self._observation):
                    self._observation = change_to_composite(self._observation)
                    self._logger.info('Changing from Simple to Composite for {self._observation.observation_id}')
                self._update_provenance(self._observation)

        if (
            self._observation.instrument is None
            or self._observation.instrument.name is None
            or len(self._observation.instrument.name) == 0
        ):
            self._update_instrument_name(self._observation)

        self._logger.debug('Done update.')
        return self._observation

    def _update_energy(self, chunk):
        """Create SpectralWCS information using FITS headers, if available. If
        the WLEN and BANDPASS keyword values are set to the defaults, there is
        no energy information."""
        self._logger.debug('Begin _update_energy')
        check_param(chunk, Chunk)

        wlen = self._headers[0].get('WLEN')
        bandpass = self._headers[0].get('BANDPASS')
        if wlen is None or wlen < 0 or bandpass is None or bandpass < 0:
            chunk.energy = None
            chunk.energy_axis = None
            self._logger.debug(
                f'Setting chunk energy to None because WLEN {wlen} and '
                f'BANDPASS {bandpass}'
            )
        else:
            naxis = CoordAxis1D(Axis('WAVE', 'um'))
            start_ref_coord = RefCoord(0.5, self.get_start_ref_coord_val(0))
            end_ref_coord = RefCoord(1.5, self.get_end_ref_coord_val(0))
            naxis.range = CoordRange1D(start_ref_coord, end_ref_coord)
            chunk.energy = SpectralWCS(
                naxis,
                specsys='TOPOCENT',
                ssysobs='TOPOCENT',
                ssyssrc='TOPOCENT',
                bandpass_name=self._headers[0].get('FILTER'),
            )
            chunk.energy_axis = None
            self._logger.debug('Setting chunk energy range (CoordRange1D).')

    def _update_instrument_name(self, observation):
        """Hard-code instrument name, if it's not specified in header
        keywords."""
        if observation.observation_id.startswith('C'):
            name = 'CPAPIR'
        elif observation.observation_id.startswith('P'):
            name = 'PESTO'
        elif observation.observation_id.startswith('S'):
            name = 'SPIOMM'
        else:
            raise CadcException(
                f'Unexpected observation id format: '
                f'{observation.observation_id}'
            )
        observation.instrument = Instrument(name)

    def _update_observation_type(self, observation):
        """For the case where a SimpleObservation needs to become a
        CompositeObservation."""
        return DerivedObservation(
            observation.collection,
            observation.observation_id,
            Algorithm('composite'),
            observation.sequence_number,
            observation.intent,
            observation.type,
            observation.proposal,
            observation.telescope,
            observation.instrument,
            observation.target,
            observation.meta_release,
            observation.meta_read_groups,
            observation.planes,
            observation.environment,
            observation.target_position,
        )

    def _update_time(self, chunk, obs_id):
        """Create TemporalWCS information using FITS header information.
        This information should always be available from the file."""
        self._logger.debug('Begin _update_time.')
        check_param(chunk, Chunk)

        mjd_start = self._headers[0].get('MJD_STAR')
        mjd_end = self._headers[0].get('MJD_END')
        if mjd_start is None or mjd_end is None:
            mjd_start, mjd_end = ac.find_time_bounds(self._headers)
        if mjd_start is None or mjd_end is None:
            chunk.time = None
            self._logger.debug(
                f'Cannot calculate MJD_STAR {mjd_start} or '
                f'MDJ_END'
                f' {mjd_end}'
            )
        elif mjd_start == 'NaN' or mjd_end == 'NaN':
            raise CadcException(
                f'Invalid time values MJD_STAR {mjd_start} or MJD_END '
                f'{mjd_end} for {obs_id}, stopping ingestion.'
            )
        else:
            self._logger.debug(
                f'Calculating range with start {mjd_start} and end {mjd_end}.'
            )
            start = RefCoord(0.5, mjd_start)
            end = RefCoord(1.5, mjd_end)
            time_cf = CoordFunction1D(1, self._headers[0].get('TEFF'), start)
            time_axis = CoordAxis1D(Axis('TIME', 'd'), function=time_cf)
            time_axis.range = CoordRange1D(start, end)
            chunk.time = TemporalWCS(time_axis)
            chunk.time.exposure = self._headers[0].get('TEFF')
            chunk.time.resolution = 0.1
            chunk.time.timesys = 'UTC'
            chunk.time.trefpos = 'TOPOCENTER'
            chunk.time_axis = None
        self._logger.debug('Done _update_time.')

    def _update_position(self, plane, intent, chunk):
        """Check that position information has been set appropriately.
        Reset to null if there's bad input data.

        DD - 19-03-20 - slack
        There are OMM observations with no WCS information, because the
        astrometry software did not solve. The lack of solution may have been
        because of cloud cover or because a field is just not very populated
        with stars, like near the zenith, but the data still has value.

        The OMM opinion:
        When, for SCI files only, there is no WCS solution, it means that the
        image is really bad and we should classify it junk.

        """
        self._logger.debug('Begin _update_position')
        check_param(chunk, Chunk)

        w = wcs.WCS(self._headers[0])

        if (
            chunk.position is not None
            and chunk.position.axis is not None
            and chunk.position.axis.function is None
        ) or (
            numpy.allclose(w.wcs.crval[0], 0.0)
            and numpy.allclose(w.wcs.crval[1], 0)
        ):
            chunk.position = None
            chunk.position_axis_1 = None
            chunk.position_axis_2 = None
            if intent is ObservationIntentType.SCIENCE:
                self._logger.warning(
                    f'No spatial WCS. Classifying plane '
                    f'{plane.product_id} as JUNK.'
                )
                plane.quality = DataQuality(Quality.JUNK)
            self._logger.debug(
                'Removing the partial position record from the chunk.'
            )

        self._logger.debug('End _update_position')

    def _update_provenance(self, observation):
        """The provenance information in the reduced product headers is not
        patterned according to the content of ad. Make the provenance information
        conform, at the Observation and Plane level.

        In the CALRED files, the provenance information is available via
        header keywords.
        """
        self._logger.debug(
            f'Begin _update_provenance for {observation.observation_id} '
            f'with {observation.intent}.'
        )

        if observation.intent is ObservationIntentType.SCIENCE:
            self._update_science_provenance(observation)
        else:
            self._update_cal_provenance(observation)

        self._logger.debug('End _update_provenance')

    def _update_science_provenance(self, observation):
        members_inputs = TypedSet(
            ObservationURI,
        )
        plane_inputs = TypedSet(
            PlaneURI,
        )
        # values look like:
        # IN_00010= 'S/data/cpapir/data/101116/101116_0088.fits.fits.gz'
        # or
        # IN_00001= 'S050213_0278.fits.gz' /raw input file (1/5)
        # or
        # DD - slack - 11-02-20
        # Add this new prefix. This will be a much easier fix than changing the
        # pipeline and all the headers once more.
        #
        # ID_00001= 'S/data/cpapir/data/101116/101116_0041.fits.fits.gz'
        for keyword in self._headers[0]:
            if keyword.startswith('IN_') or keyword.startswith('ID_'):
                temp = keyword.split('_')[1]
                try:
                    int(temp)
                except ValueError as e:
                    # skip the keyword ID_PROG
                    continue
                value = self._headers[0].get(keyword)
                base_name = OmmName.remove_extensions(os.path.basename(value))
                if base_name.startswith('S'):
                    # starting 'S' means a science input, 'C' will mean cal
                    base_name = base_name.replace('S', 'C', 1)
                    file_id = f'{base_name}_SCI'
                elif value.startswith('S'):
                    base_name = f'C{base_name}'
                    file_id = f'{base_name}_SCI'
                elif base_name.startswith('C') or value.startswith('C'):
                    file_id = f'{base_name}_CAL'
                else:
                    # don't fail when values look like this:
                    # ID_00001= ' S      '           / raw input file
                    self._logger.warning(f'Unknown provenance file naming pattern {base_name}')
                    continue

                obs_member_uri_str = CaomName.make_obs_uri_from_obs_id(self._storage_name.collection, base_name)
                obs_member_uri = ObservationURI(obs_member_uri_str)
                plane_uri = PlaneURI.get_plane_uri(obs_member_uri, file_id)
                plane_inputs.add(plane_uri)
                members_inputs.add(obs_member_uri)

        update_typed_set(observation.members, members_inputs)
        for plane in observation.planes.values():
            update_typed_set(plane.provenance.inputs, plane_inputs)

    def _update_cal_provenance(self, observation):
        plane_inputs = TypedSet(
            PlaneURI,
        )
        members_inputs = TypedSet(
            ObservationURI,
        )
        for keyword in self._headers[0]:
            if keyword.startswith('F_ON') or keyword.startswith('F_OFF'):
                value = self._headers[0].get(keyword)
                base_name = (
                    f'C{OmmName.remove_extensions(os.path.basename(value))}'
                )
                file_id = f'{base_name}_CAL'

                obs_member_uri_str = CaomName.make_obs_uri_from_obs_id(self._storage_name.collection, base_name)
                obs_member_uri = ObservationURI(obs_member_uri_str)
                plane_uri = PlaneURI.get_plane_uri(obs_member_uri, file_id)
                plane_inputs.add(plane_uri)
                members_inputs.add(obs_member_uri)

        for plane in observation.planes.values():
            if plane.provenance is None:
                plane.provenance = Provenance('CPAPIR')
            update_typed_set(plane.provenance.inputs, plane_inputs)

        update_typed_set(observation.members, members_inputs)

    def _update_requirements(self, observation):
        """
        Add Requirements Status FAIL to observations that are named _REJECT.
        Hard-code the target name to 'BAD' in this case, as well.

        :param observation: Observation.requirements.status
        """
        observation.requirements = Requirements(Status.FAIL)
        observation.target.name = 'BAD'

    def _update_time_bounds(self, observation, storage_name):
        """Add chunk time bounds to the chunk from the first part, by
        referencing information from the second header."""

        lower_values = ''
        upper_values = ''
        with fits.open(storage_name.sources_names[0]) as fits_data:
            xtension = fits_data[1].header['XTENSION']
            extname = fits_data[1].header['EXTNAME']
            if 'BINTABLE' in xtension and 'PROVENANCE' in extname:
                for ii in fits_data[1].data[0]['STARTTIME']:
                    lower_values = f'{ii} {lower_values}'
                for ii in fits_data[1].data[0]['DURATION']:
                    upper_values = f'{ii} {upper_values} '
            else:
                raise CadcException(
                    f'Opened a composite file that does not match the '
                    f'expected profile '
                    f'(XTENSION=BINTABLE/EXTNAME=PROVENANCE). '
                    f'{xtension} {extname}'
                )

        for plane in observation.planes:
            for artifact in observation.planes[plane].artifacts:
                parts = observation.planes[plane].artifacts[artifact].parts
                for p in parts:
                    if p == '0':
                        lower = lower_values.split()
                        upper = upper_values.split()
                        if len(lower) != len(upper):
                            raise CadcException(
                                'Cannot make RefCoords with inconsistent '
                                'values.'
                            )
                        chunk = parts[p].chunks[0]
                        bounds = CoordBounds1D()
                        chunk.time.axis.bounds = bounds
                        for ii in range(len(lower)):
                            mjd_start, mjd_end = ac.convert_time(
                                to_float(lower[ii]), to_float(upper[ii])
                            )
                            lower_refcoord = RefCoord(0.5, mjd_start)
                            upper_refcoord = RefCoord(1.5, mjd_end)
                            r = CoordRange1D(lower_refcoord, upper_refcoord)
                            bounds.samples.append(r)
                        # if execution has gotten to this point, remove range
                        # if it exists, since only one of bounds or range
                        # should be provided, and bounds is more specific. PD,
                        # slack, 2018-07-16
                        if chunk.time.axis.range is not None:
                            chunk.time.axis.range = None

    def _update_telescope_location(self, observation):
        """Provide geocentric telescope location information, based on
        geodetic information from the headers."""

        self._logger.debug('Begin _update_telescope_location')
        if not isinstance(observation, Observation):
            raise CadcException('Input type is Observation.')

        telescope = self._headers[0].get('TELESCOP')

        if telescope is None:
            self._logger.warning(
                f'No telescope name. Could not set telescope '
                f'location for {observation.observation_id}'
            )
            return

        telescope = telescope.upper()
        if self._storage_name.collection in telescope or 'CTIO' in telescope:
            lat = self._headers[0].get('OBS_LAT')
            long = self._headers[0].get('OBS_LON')

            # make a reliable lookup value
            if self._storage_name.collection in telescope:
                telescope = self._storage_name.collection
            if 'CTIO' in telescope:
                telescope = 'CTIO'

            if lat is None or long is None:
                observation.telescope.geo_location_x = DEFAULT_GEOCENTRIC[
                    telescope
                ]['x']
                observation.telescope.geo_location_y = DEFAULT_GEOCENTRIC[
                    telescope
                ]['y']
                observation.telescope.geo_location_z = DEFAULT_GEOCENTRIC[
                    telescope
                ]['z']
            else:
                (
                    observation.telescope.geo_location_x,
                    observation.telescope.geo_location_y,
                    observation.telescope.geo_location_z,
                ) = ac.get_location(
                    lat, long, DEFAULT_GEOCENTRIC[telescope]['elevation']
                )
        else:
            raise CadcException(f'Unexpected telescope name {telescope}')

        self._logger.debug('Done _update_telescope_location')

    @staticmethod
    def change_type(observation):
        """OMM has the case where there are SimpleObservation instances that will change type to CompositeObservation
        instances. This methods finds those cases."""
        result = False
        if isinstance(observation, SimpleObservation):
            if len(observation.planes) > 1:
                # even if there is no need for delete, it just takes longer, the Observation won't end up in an
                # in-correct state, so be more lenient than less in determining whether or not an observation needs
                # deleting
                result = True
            else:
                for plane in observation.planes.values():
                    if OmmName.is_composite(plane.product_id):
                        result = True
                        break
        return result
