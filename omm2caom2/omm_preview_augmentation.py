import logging
import os
import subprocess

from hashlib import md5

from caom2 import Artifact, ProductType, ReleaseType, ChecksumURI
from caom2 import Observation

__all__ = ['visit']


def visit(observation, **kwargs):
    # TODO - make sure the file exists here, make sure the directory locations
    # are somehow known

    assert observation is not None, 'Input parameter must have a value.'
    assert isinstance(observation, Observation), \
        'Input parameter must be an Observation'

    working_dir = './'
    if 'working_directory' in kwargs:
        working_dir = kwargs['working_directory']
    if 'netrc_fqn' in kwargs:
        netrc_fqn = kwargs['netrc_fqn']

    for i in observation.planes:
        plane = observation.planes[i]
        for j in plane.artifacts:
            artifact = plane.artifacts[j]
            # assume the file exists on disk - it's been put there by a
            # DAG task for this
            if (artifact.uri.endswith('.fits.gz') or artifact.uri.endswith(
                    '.fits')):
                fn = artifact.uri.split('/')[1]
                file_name = fn.split('.gz')[0]
                file_id = file_name.split('.fits')[0]
                logging.debug(
                    'working on file id {} in working directory {} {}'.format(
                        file_id, working_dir, file_name))
                # all the code is written to append .fits right now ....
                _do_prev(file_id, file_name, working_dir)
                _put_omm_preview(file_id, netrc_fqn)
                _augment_plane(plane, file_id, working_dir)

    return {'artifacts': 2}


def _augment_plane(plane, file_id, working_dir):
    preview_jpg = file_id + '_prev.jpg'
    preview_jpg256 = file_id + '_prev_256.jpg'
    plane.artifacts.add(
        _artifact_metadata(preview_jpg, working_dir, ProductType.PREVIEW))
    plane.artifacts.add(
        _artifact_metadata(preview_jpg256, working_dir, ProductType.THUMBNAIL))


def _artifact_metadata(file_name, working_dir, product_type):
    fqfn = os.path.join(working_dir, file_name)
    md5uri = ChecksumURI(md5(open(fqfn, 'rb').read()).hexdigest())
    return Artifact('ad:OMM/{}'.format(file_name), product_type,
                    ReleaseType.DATA, content_type='image/jpg',
                    content_length=os.path.getsize(fqfn),
                    content_checksum=md5uri)


def _do_prev(file_id, file_name, working_dir):
    preview_jpg = os.path.join(working_dir, file_id) + '_prev.jpg'
    preview_jpg256 = os.path.join(working_dir, file_id) + '_prev_256.jpg'
    fqfn = '{}'.format(os.path.join(working_dir, file_name))

    # TODO when you want better feedback from system command execution, replace
    # these calls with subprocess stuff
    # merci

    if os.access(preview_jpg, 0): os.remove(preview_jpg)
    # prev_cmd = 'fitscut --all --autoscale=99.5 --asinh-scale --jpg --invert --compass ' + file_name + '.fits >' + preview_jpg
    prev_cmd = 'fitscut --all --autoscale=99.5 --asinh-scale --jpg --invert ' \
               '--compass {} >{}'.format(fqfn, preview_jpg)
    status = os.system(prev_cmd)
    if status != 0:
        logging.error('Executing command {} exited with {}'.format(prev_cmd,
                                                                   status))
    if os.access(preview_jpg256, 0): os.remove(preview_jpg256)
    # prev_cmd = 'fitscut --all --output-size=256 --autoscale=99.5 --asinh-scale --jpg --invert --compass ' + file_name + '.fits >' + preview_jpg256
    prev_cmd = 'fitscut --all --output-size=256 --autoscale=99.5 ' \
               '--asinh-scale --jpg --invert --compass {}>{}'.format(
        fqfn, preview_jpg256)
    status = os.system(prev_cmd)
    if status != 0:
        logging.error('Executing command {} exited with {}'.format(prev_cmd,
                                                                   status))
    logging.debug('Completed preview generation for {}.'.format(file_id))


def _put_omm_preview(file_name, netrc_fqn):
    # return
    # TODO - make this work - how to test it lol?

    preview_jpg = file_name + '_prev.jpg'
    preview_jpg256 = file_name + '_prev_256.jpg'

    # cmd = ['cadc-data', 'put', '--cert', _MYCERT, 'OMM', preview_jpg256, '--archive-stream', 'raw']
    cmd = ['cadc-data', 'put', '--netrc-file', netrc_fqn, 'OMM', preview_jpg256,
           '--archive-stream', 'raw']
    print('preview put: ', cmd)
    try:
        output = subprocess.Popen(cmd, stdout=subprocess.PIPE).communicate()[
            0].strip()
    except Exception as e:
        print("ERROR: could not put in archive FileName %s!!! %s" % (
            preview_jpg256, str(e)))

    # cmd = ['cadc-data', 'put', '--cert', _MYCERT, 'OMM', preview_jpg, '--archive-stream', 'raw']
    cmd = ['cadc-data', 'put', '--netrc-file', netrc_fqn, 'OMM', preview_jpg,
           '--archive-stream', 'raw']
    print('preview put: ', cmd)
    try:
        output = subprocess.Popen(cmd, stdout=subprocess.PIPE).communicate()[
            0].strip()
    except Exception as e:
        print("ERROR: could not put in archive FileName %s!!! %s" % (
            preview_jpg, str(e)))
