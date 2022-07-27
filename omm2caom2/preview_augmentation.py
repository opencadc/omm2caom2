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

from caom2 import ProductType, ReleaseType
from caom2pipe import manage_composable as mc
from omm2caom2 import COLLECTION
from astropy.visualization import ImageNormalize, PowerStretch, ZScaleInterval
import numpy as np
import matplotlib.pyplot as plt
from astropy.io.fits import open
from astropy.wcs import WCS


__all__ = ['visit']


class OMMPreview(mc.PreviewVisitor):
    def __init__(self, **kwargs):
        super().__init__(
            COLLECTION, ReleaseType.DATA, mime_type='image/jpeg', **kwargs
        )

    def generate_plots(self, obs_id):
        self._logger.debug(f'Begin generate_plots for {obs_id}')
        count = 0
        # algorithm - @Nat1405, @dbohlender
        # quality control and review - @dnarudd
        hdus = open(self._science_fqn)
        wcs = WCS(hdus[0].header)
        data = hdus[0].data.copy()

        # see https://docs.astropy.org/en/stable/io/
        # fits/index.html#working-with-large-files
        hdus.close(self._science_fqn)
        del hdus[0].data
        del hdus

        plt.figure(figsize=(10.24, 10.24), dpi=200)
        plt.grid(False)
        plt.axis('off')
        plt.subplot(projection=wcs)
        interval = ZScaleInterval()
        white_light_data = interval(np.flipud(data))
        norm = ImageNormalize(
            white_light_data,
            interval=ZScaleInterval(),
            stretch=PowerStretch(3),
        )
        plt.imshow(white_light_data, cmap='binary', norm=norm)
        plt.savefig(
            self._preview_fqn, dpi=200, bbox_inches='tight', format='jpg'
        )

        count += 1
        self.add_preview(
            self._storage_name.prev_uri,
            self._storage_name.prev,
            ProductType.PREVIEW,
        )
        self.add_to_delete(self._preview_fqn)
        count += self._gen_thumbnail()
        self.add_preview(
            self._storage_name.thumb_uri,
            self._storage_name.thumb,
            ProductType.THUMBNAIL,
        )
        self.add_to_delete(self._thumb_fqn)
        self._logger.info(f'End generate_plots for {obs_id}.')
        return count


def visit(observation, **kwargs):
    return OMMPreview(**kwargs).visit(observation)
