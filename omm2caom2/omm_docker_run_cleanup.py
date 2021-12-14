#!/usr/bin/env python

#
# The cleanup procedure for the OMM docker run. Have to be run instead of the omm_run
#
import os
import fnmatch
import glob

# Getting a list of all the files in the current directory and in the logs
all_files = glob.glob('**', recursive=True)

for afile in all_files:
    # identifying the files created by the footprintfinder
    if fnmatch.fnmatch(afile, '*footprint*'):
        # print('todelete', afile)
        os.remove(afile)
    # indentifying the regular preview files
    elif fnmatch.fnmatch(afile, '*_prev.jpg'):
        # print('todelete', afile)
        os.remove(afile)
    # identifying the postage stamp preview
    elif fnmatch.fnmatch(afile, '*_prev_256.jpg'):
        # print('todelete', afile)
        os.remove(afile)
    # identifying the CAOM xml file
    elif fnmatch.fnmatch(afile, '*fits.xml'):
        # print('todelete', afile)
        os.remove(afile)
    # indentifying the fits or the fits.gz files
    # we are deleting the *.fits ONLY if there is an equivalent *.fits.gz
    elif fnmatch.fnmatch(afile, '*.fits'):
        # print('afile: ', afile)
        base = os.path.basename(afile)
        # print('base: ', base)
        rootname = os.path.splitext(base)[0]
        # print('rootname before: ', rootname)
        rootname = rootname + '.fits.gz'
        # print('rootname after: ', rootname, afile)
        if os.path.isfile(rootname) & os.path.isfile(afile):
            # print('todelete', afile)
            os.remove(afile)
