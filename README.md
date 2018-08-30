# omm2caom2
Application to generate a CAOM2 observation from OMM FITS files

# How To Run OMM Testing

In an empty directory:

1. This is the working directory, so it should probably have some space.

1. In the master branch of this repository, find the files Dockerfile, 
docker-entrypoint.sh, and config.yml. Copy these files to the working directory.

1. Make docker-entrypoint.sh executable.

1. config.yml is configuration information for the ingestion. It will work with 
the files named and described here. For a complete description of its
content, see 
https://github.com/opencadc-metadata-curation/collection2caom2/wiki/config.yml.

1. In this directory, create a file name test_netrc. This is the expected 
.netrc file that will have the credentials required for the caom2repo and 
data services. These credentials allow the user to read, write, and delete 
CAOM2 observations using caom2repo, and read file header metadata and files 
from data. This file should have content that looks like the following:

   ```
   machine sc2.canfar.net login canfarusername password canfarpassword
   ```
   1. Replace sc2.canfar.net with the hostname for the service that requires 
   credentials.
   
   1. Replace canfarusername and canfarpassword with your CADC username and 
   password values.

   1. The permissions for this file must be 600 (owner rw only).
   
   1. The man page for netrc:
   https://www.systutorials.com/docs/linux/man/5-netrc/
   
   1. The name and location of this file may be changed by modifying the 
   netrc_filename entry in the config.yml file. This entry requires a 
   fully-qualified pathname.

1. The ways to tell this tool the work to be done:

   1. provide a file containing the list of file ids to process, one file id 
   per line, and the config.yml file containing the entries 'use_local_files' 
   set to False, and 'task_types' set to -ingest -modify. The 'todo' 
   file may provided in one of two ways:
      1. named 'todo.txt' in this directory, as specified in config.yml, or
      1. as the fully-qualified name with the --todo parameter

   1. provide the files to be processed in the working directory, and the 
   config.yml file containing the entries 'use_local_files' set to True, 
   and 'task_types' set to -store -ingest -modify.
      1. The store task does not have to be present, unless the files on disk 
      are newer than the same files at CADC.

1. To build the container image, run this:

   ```
   docker build -f Dockerfile -t omm_run_cli ./
   ```

1. To run the application:

   ```
   user@dockerhost:<cwd># docker run --rm -ti -v <cwd>:/usr/src/app --name omm_run_cli omm_run_cli omm_run
   ```

1. To run the application with the todo file as a command-line parameter:

   ```
   user@dockerhost:<cwd># docker run --rm -ti -v <cwd>:/usr/src/app --name omm_run_cli omm_run_cli omm_run --todo list_of_obs_ids.txt
   ```

1. To debug the application from inside the container:

   ```
   user@dockerhost:<cwd># docker run --rm -ti -v <cwd>:/usr/src/app --name omm_run_cli omm_run_cli /bin/bash
   root@53bef30d8af3:/usr/src/app# omm_run
   ```

1. For some instructions that might be helpful on using containers, see:
https://github.com/opencadc-metadata-curation/collection2caom2/wiki/Docker-and-Collections
