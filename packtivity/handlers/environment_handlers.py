import os
import subprocess
import sys
import time
import psutil
import utils
import logging

handlers,environment = utils.handler_decorator()

def prepare_docker(nametag,workdir,do_cvmfs,do_grid):
    docker_mod = ''
    if 'PACKTIVITY_WORKDIR_LOCATION' not in os.environ:
        docker_mod += '-v {}:/workdir'.format(os.path.abspath(workdir))
    else:
        docker_mod += '-v {}:/workdir'.format(os.environ['PACKTIVITY_WORKDIR_LOCATION'])        
        
    if do_cvmfs:
        if 'PACKTIVITY_CVMFS_LOCATION' not in os.environ:
            docker_mod+=' -v /cvmfs:/cvmfs'
        else:
            docker_mod+=' -v {}:/cvmfs'.format(os.environ['YADAGE_CVMFS_LOCATION'])
    if do_grid:
        if 'PACKTIVITY_AUTH_LOCATION' not in os.environ:
            docker_mod+=' -v /home/recast/recast_auth:/recast_auth'
        else:
            docker_mod+=' -v {}:/recast_auth'.format(os.environ['YADAGE_AUTH_LOCATION'])
            
    docker_mod += ' --cidfile {}/{}.cid'.format(workdir,nametag)
    
    return docker_mod
    
@environment('docker-encapsulated')
def docker_enc_handler(nametag,environment,context,command):
    log  = logging.getLogger('step_logger_{}'.format(nametag))
    logname = '{}/{}.step.log'.format(os.path.abspath(context['workdir']),nametag)
    fh  = logging.FileHandler(logname)
    fh.setLevel(logging.DEBUG)
    log.addHandler(fh)
    log.debug('starting log for step: %s',nametag)
    
    log.debug('context: \n {}'.format(context))
    workdir = context['workdir']
    
    container = environment['image']
    
    report = '''\n\
--------------
run in docker container: {container}
with env: {env}
command: {command}
resources: {resources}
--------------
    '''.format(container = container,
               command = command,
               env = environment['envscript'] if environment['envscript'] else 'default env',
               resources = environment['resources']
              )
  
    do_cvmfs = 'CVMFS' in environment['resources']
    do_grid  = 'GRIDProxy'  in environment['resources']
    
    log.debug(report)
    log.debug('dogrid: {} do_cvmfs: {}'.format(do_grid,do_cvmfs))
    
    envmod = 'source {} &&'.format(environment['envscript']) if environment['envscript'] else ''
    
    in_docker_cmd = '{envmodifier} {command}'.format(envmodifier = envmod, command = command)
    
    docker_mod = prepare_docker(nametag,workdir,do_cvmfs,do_grid)
    
    fullest_command = 'docker run --rm {docker_mod} {container} sh -c \'{in_dock}\''.format(
                        docker_mod = docker_mod,
                        container = container,
                        in_dock = in_docker_cmd
                        )
    if do_cvmfs:
        if not 'PACKTIVITY_WITHIN_DOCKER' in os.environ:
            fullest_command = 'cvmfs_config probe && {}'.format(fullest_command)
    
    docker_pull_cmd = 'docker pull {container}'.format(container = container)
    
    log.debug('docker pull command: \n  {}'.format(docker_pull_cmd))
    log.debug('docker run  command: \n  {}'.format(fullest_command))
    
    try:
      with open('{}/{}.pull.log'.format(workdir,nametag),'w') as logfile:
        proc = subprocess.Popen(docker_pull_cmd,shell = True, stderr = subprocess.STDOUT, stdout = logfile)
        log.debug('started pull subprocess with pid {}. now wait to finish'.format(proc.pid))
        time.sleep(0.5)
        log.debug('process children: {}'.format([x for x in psutil.Process(proc.pid).children(recursive = True)]))
        proc.communicate()
        log.debug('pull subprocess finished. return code: {}'.format(proc.returncode))
        if proc.returncode:
          log.error('non-zero return code raising exception')
          raise subprocess.CalledProcessError(returncode =  proc.returncode, cmd = docker_pull_cmd)
        log.debug('moving on from pull')
    except RuntimeError as e:
      log.exception('caught RuntimeError')
      raise e
    except subprocess.CalledProcessError as exc:
      log.exception('subprocess failed. code: {},  command {}'.format(exc.returncode,exc.cmd))
      raise RuntimeError('failed docker subprocess in docker_enc_handler.')
    except:
      log.exception("Unexpected error: {}".format(sys.exc_info()))
      raise
    finally:
      log.debug('finally for pull')

    try:
        with open('{}/{}.run.log'.format(workdir,nametag),'w') as logfile:
            proc = subprocess.Popen(fullest_command,shell = True, stderr = subprocess.STDOUT, stdout = logfile)
            log.debug('started run subprocess with pid {}. now wait to finish'.format(proc.pid))
            time.sleep(0.5)
            log.debug('process children: {}'.format([x for x in psutil.Process(proc.pid).children(recursive = True)]))
            proc.communicate()
            log.debug('pull subprocess finished. return code: {}'.format(proc.returncode))
            if proc.returncode:
                log.error('non-zero return code raising exception')
                raise subprocess.CalledProcessError(returncode =  proc.returncode, cmd = fullest_command)
            log.debug('moving on from run')
    except subprocess.CalledProcessError as exc:
        log.exception('subprocess failed. code: {},  command {}'.format(exc.returncode,exc.cmd))
        raise RuntimeError('failed docker subprocess in docker_enc_handler.')
    except:
        log.exception("Unexpected error: {}".format(sys.exc_info()))
        raise
    finally:
        log.debug('finally for run')
    log.debug('reached return for docker_enc_handler')