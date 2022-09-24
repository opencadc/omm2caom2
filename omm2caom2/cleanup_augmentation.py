# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2020.                            (c) 2020.
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

from datetime import datetime
from slack import WebClient
from slack.errors import SlackApiError

from caom2 import Observation
from caom2pipe import manage_composable as mc
from omm2caom2 import OmmName


def visit(observation, **kwargs):
    """
    Clean up the issue described here (multiple planes for the same photons):
    https://github.com/opencadc-metadata-curation/omm2caom2/issues/3
    """
    mc.check_param(observation, Observation)
    logging.info(
        f'Begin cleanup augmentation for ' f'{observation.observation_id}'
    )
    clients = kwargs.get('clients')
    count = 0
    if clients is None or clients.data_client is None:
        logging.warning(
            'Stopping. Need a CADC Client for cleanup augmentation.'
        )
    else:
        if len(observation.planes) > 1:
            # from Daniel, Sylvie - 21-05-20
            # How to figure out which plane is newer:
            # SB - I do not think that we should use the “VERSION” keyword.
            # I think we must go with the ingested date.
            #
            # Daniel Durand
            # Might be better indeed. Need to compare the SCI and the REJECT
            # file and see which one is the latest

            latest_plane_id = None
            latest_timestamp = None
            temp = []
            for plane in observation.planes.values():
                for artifact in plane.artifacts.values():
                    if OmmName.is_preview(artifact.uri):
                        continue
                    meta = clients.data_client.info(artifact.uri)
                    if meta is None:
                        logging.warning(
                            f'Did not find {artifact.uri} in CADC storage.'
                        )
                    else:
                        if latest_plane_id is None:
                            latest_plane_id = plane.product_id
                            if isinstance(meta.lastmod, datetime):
                                latest_timestamp = meta.lastmod.timestamp()
                            else:
                                latest_timestamp = mc.make_time(meta.lastmod)
                        else:
                            if isinstance(meta.lastmod, datetime):
                                current_timestamp = meta.lastmod.timestamp()
                            else:
                                current_timestamp = mc.make_time(meta.lastmod)
                            if current_timestamp > latest_timestamp:
                                latest_timestamp = current_timestamp
                                temp.append(latest_plane_id)
                                latest_plane_id = plane.product_id
                            else:
                                temp.append(plane.product_id)

            delete_list = list(set(temp))
            for entry in delete_list:
                logging.warning(
                    f'Removing plane {entry} from observation '
                    f'{observation.observation_id}. There are '
                    f'duplicate photons.'
                )
                count += 1
                observation.planes.pop(entry)
                _send_slack_message(entry)

    logging.info(
        f'Completed cleanup augmentation for ' f'{observation.observation_id}'
    )
    return observation


def _send_slack_message(entry):
    config = mc.Config()
    config.get_executors()
    client = WebClient(token=config.slack_token)

    msg = f'Delete OMM {entry}.fits.gz'
    try:
        ignore = client.chat_postMessage(
            channel=config.slack_channel, text=msg
        )
    except SlackApiError as sae:
        logging.error(
            f'Could not sent slack message {msg} to ' f'{config.slack_channel}'
        )
        logging.debug(sae)
