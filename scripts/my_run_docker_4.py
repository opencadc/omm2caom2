#!/usr/bin/env python

# Location of the temporary working directory
_WORKPATH = '/space/omm2cadc/archivage2018/'

# Location of where the original files are stored. /data/cpapir/data/ on the main server
#_DATAPATH = '/data/cpapir/data/'
_DATAPATH = '/data/cpapir/data/'

# Location of the logs path
#_LOGPATH = '/archives2018/logs/'
_LOGPATH = '/space/omm2cadc/archivage2018/logs/'

#Docker run command (sudo is required on my test host)
#_DOCKER_RUN = 'sudo docker run'
_DOCKER_RUN = 'docker run'

import os
import sys
import shutil

def do_me( my_docker_template, my_docker, my_night ):

   #create a local directory with the name of the night as given by the argument
   my_cwd = os.getcwd()
   #my_todo = os.path.basename(my_todo_path)
   os.chdir(_WORKPATH)
   try:
      os.mkdir(my_night, 0o777)
   except:
      print('Directory '+_WORKPATH+my_night+' already exists')

   #my_working_dir = cwd+'/'+my_night
   os.chmod(my_night, 0o777)
   os.chdir(my_night)

   #In this working directory, copy the observation fits file while renaming them properly
   #according to the value of their DATATYPE FITS key
   #All the files in the input directory will be processed
   #If the DATATYPE key is not found, the FITS file is NOT process and will appear in the night_ERROR file
   #Any valid FITS file with be stored in the todo.txt list
   
   list_of_files = get_list_of_files( my_night )

   my_working_dir = os.getcwd()
   if os.access('todo.txt',0 ):
      os.remove('todo.txt')
   for my_file in list_of_files:
      print( my_file )
      new_name = create_new_name( my_file, str(my_night) )
      if new_name == 'error':
         continue
      cmd = 'ln -s '+my_file+' '+new_name
      rootname = my_file.strip('.fits.gz')
      print (rootname, file=open("todo.txt", "a"))
      print(cmd)
      try:
         os.system( cmd )
      except:
         red('Link already exists \n')

   # generate the cadcproxy.pem file 
   proxy_cmd = _DOCKER_RUN+" -v "+my_working_dir+":/usr/src/app --rm --name "+my_night+" -ti "+my_docker+" cadc-get-cert --days-valid 10 -u beaulieusf"
   print (proxy_cmd)
   os.system( proxy_cmd )

   # Copy the docker template as specified on the cammand line
   cmd = "cp -p "+my_docker_template+"/config.yml "+" ."
   os.system( cmd )
   cmd = "cp -p "+my_docker_template+"/docker-entrypoint.sh "+" ."
   os.system( cmd )

   # Change the log file to reflec the night number
   #cmd = "sed -i.tmp 's_/usr/src/app/logs_"+_LOGPATH+my_night+"-log_' config.yml"
   #print (cmd)
   #os.system( cmd )

   # run the docker instance (sudo is required on my test machine and it is not needed on maestria)
   docker_cmd = _DOCKER_RUN+" -v "+my_working_dir+":/usr/src/app -v "+_DATAPATH+my_night+":"+_DATAPATH+my_night+" --rm --name "+my_night+" -ti "+my_docker+" omm_run"
   print (docker_cmd)
   os.system( docker_cmd )

   try:
      os.mkdir(_LOGPATH+my_night)
   except:
      red('Log directory already exists \n')
      red('Removing and re-creating \n')
      shutil.rmtree( _LOGPATH+my_night)
      os.mkdir(_LOGPATH+my_night)


   # Remove any old logs_0 because if fixed they are corrupting the statistics
   if os.path.exists( _LOGPATH+my_night+'/logs_0' ):
      shutil.rmtree( _LOGPATH+my_night+'/logs_0' )
    

   cmd = 'cp -rp logs/* '+_LOGPATH+my_night
   print ('Executing: ',cmd)
   os.system( cmd )
   if os.path.exists( 'logs_0'):
      cmd = 'cp -rp logs_0 '+_LOGPATH+my_night+'/'
      print ('Executing: ',cmd)
      os.system( cmd )

   print("If require to run this docker interactively do: ")
   docker_cmd = _DOCKER_RUN+" -v "+my_working_dir+":/usr/src/app -v "+_DATAPATH+my_night+":"+_DATAPATH+my_night+" --rm --name "+my_night+" -ti "+my_docker+" /bin/bash"
   print(docker_cmd)

   print("The log files for this past run are available in : ",_LOGPATH+my_night)

   print("To clean the docker directory, please issue the following command:")
   bash_cmd = "./my_run_clean.py "+my_night
   #docker_cmd = _DOCKER_RUN+" -v "+my_working_dir+":/usr/src/app -v "+_DATAPATH+my_night+":"+_DATAPATH+my_night+" --rm --name "+my_night+" -ti "+my_docker+" python /usr/local/bin/omm_docker_run_cleanup.py"
   print(bash_cmd)
   
def get_list_of_files( night ):

    #Get the list of fits.gz files in the input night directory.
    data_orig_path = _DATAPATH+night
    print (data_orig_path)
    import glob
    list_of_files = sorted(glob.glob(data_orig_path+'/'+night+'*.fits.gz'))
    good('Number of files to process is '+str(len(list_of_files))+' \n')
    return( list_of_files )

def create_new_name( filename, night ):
    import astropy
    from astropy.io import fits
    from astropy.io.fits import getheader
    _ERROR_FILE = night+'_ERRORS'
    try:
       hdr = getheader(filename)
    except:
       print('ERROR: ', filename,' is corrupted')
       if os.access(_ERROR_FILE,0):
           fh = open(_ERROR_FILE,'a')
       else:
           fh = open(_ERROR_FILE,'w')
       fh.write('ERROR: '+filename+' No DATATYPE\n')
       fh.close()
       return('error')

    if 'DATATYPE' not in hdr:
       print('ERROR: ', filename,' do not have a datatype')
       if os.access(_ERROR_FILE,0):
           fh = open(_ERROR_FILE,'a')
       else:
           fh = open(_ERROR_FILE,'w')
       fh.write('ERROR: '+filename+' No DATATYPE\n')
       fh.close()
       return('error')
    datatype = hdr['DATATYPE']
    print (datatype)
    # Have to deal with the size of the image 
    if datatype == 'CALIB'  : ext='_CAL'
    if datatype == 'SCIENCE': ext='_SCI'
    if datatype == 'FOCUS'  : 
        ext='_FOCUS'
        #we are now returning an error if we are dealing with a focus frame which should not ingest them
        return('error')
    if datatype == 'CALRED' : ext='_CALRED'
    if datatype == 'REJECT' : ext='_REJECT'
    if datatype == 'TEST'   : ext='_TEST'
    #if strpos(fic[i],'/reductions/') ne (-1) then type='_SCIRED'
    data_orig_path = _DATAPATH+night+'/'
    my_year  = '20'+night[0:2]
    my_month = night[2:4]
    my_day   = night[4:6]
    data_new_path  = './'
    new_name = filename.replace('.fits.gz',ext+'.fits.gz')
    new_name = new_name.replace(data_orig_path,data_new_path+'C')
    return (new_name)



def orange(string):
    ''' return the string in orange '''
    return "\033[39;33m" + string + "\033[39;29m"

def red(string):
    ''' return the string in red '''
    return "\033[39;31m" + string + "\033[39;29m"

def black(string):
    ''' return the string in black '''
    return  "\033[39;29m" + string + "\033[39;29m"

def green(string):
    ''' return the string in green '''
    return "\033[39;32m" + string + "\033[39;29m"

def blue(string):
    ''' return the string in blue '''
    return "\033[39;34m" + string + "\033[39;29m"

def alert(string, output=sys.stdout):
    ''' prints the string in blue.  '''
    output.write(blue(string))

def error(string, output=sys.stdout):
    ''' prints the string in red.  '''
    output.write(red(string))

def info(string, output=sys.stdout):
    ''' prints the string in red.  '''
    output.write(orange(string))

def good(string, output=sys.stdout):
    ''' prints the string in green.  '''
    output.write(green(string))


if __name__ == '__main__':
    if len(sys.argv) <= 1:
        # no args 
        print ('usage: run_my_docker.py <night_number> ')
        print()
        print("Exemple: ./my_run_docker.py 150110")
        sys.exit(1)
    else:
        do_me('/space/omm2cadc/docker/omm2caom2-master', 'omm_run_cli', sys.argv[1])




#../my_run_docker.py /staging/sciproc2-01/1/durand/temp/omm2caom2-s2235-3 omm_run_cli /staging/sciproc2-01/1/durand/temp/bla
#sudo docker run -v /staging/sciproc2-01/1/durand/temp/omm2caom2-s2235-3c:/usr/src/app --rm --name omm_run_clic -ti omm_run_cli omm_run
