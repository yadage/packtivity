import os
import subprocess
import sys
import utils
import time
import psutil
import logging
import errno

handlers,environment = utils.handler_decorator()

def sourcepath(path):
    if 'PACKTIVITY_WORKDIR_LOCATION' in os.environ:
        old,new = os.environ['PACKTIVITY_WORKDIR_LOCATION'].split(':')
        dockerpath = new+path.rsplit(old,1)[1]
        return dockerpath
    else:
        return path

def prepare_docker(nametag,context,do_cvmfs,do_grid,log):
    metadir  = context['metadir']
    readwrites  = context['readwrite']
    readonlies = context['readonly']

    docker_mod = ''
    for rw in readwrites:
        docker_mod += '-v {}:{}:rw'.format(sourcepath(os.path.abspath(rw)),rw)
    for ro in readonlies:
        docker_mod += ' -v {}:{}:ro'.format(sourcepath(ro),ro)

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
            
    cidfile = '{}/{}.cid'.format(metadir,nametag)

    if os.path.exists(cidfile):
        log.warning('cid file %s seems to exist, docker run will crash',cidfile)
    docker_mod += ' --cidfile {}'.format(cidfile)
    
    return docker_mod

def prepare_full_docker_cmd(nametag,context,environment,command,log):
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
    log.debug(report)

    do_cvmfs = 'CVMFS' in environment['resources']
    do_grid  = 'GRIDProxy'  in environment['resources']
    log.debug('dogrid: %s do_cvmfs: %s',do_grid,do_cvmfs)
    
    envmod = 'source {} &&'.format(environment['envscript']) if environment['envscript'] else ''
    
    in_docker_cmd = '{envmodifier} {command}'.format(envmodifier = envmod, command = command)
    
    docker_mod = prepare_docker(nametag,context,do_cvmfs,do_grid,log)
    
    fullest_command = 'docker run --rm {docker_mod} {container} sh -c \'{in_dock}\''.format(
                        docker_mod = docker_mod,
                        container = container,
                        in_dock = in_docker_cmd
                        )
    if do_cvmfs:
        if 'PACKTIVITY_WITHIN_DOCKER' not in os.environ:
            fullest_command = 'cvmfs_config probe && {}'.format(fullest_command)
    return fullest_command

def docker_pull(docker_pull_cmd,log,context,nametag):
    log.debug('docker pull command: \n  %s',docker_pull_cmd)
    if 'PACKTIVITY_DRYRUN' in os.environ:
        return

    metadir  = context['metadir']
    try:
        with open('{}/{}.pull.log'.format(metadir,nametag),'w') as logfile:
            proc = subprocess.Popen(docker_pull_cmd,shell = True, stderr = subprocess.STDOUT, stdout = logfile)
            log.debug('started pull subprocess with pid %s. now wait to finish',proc.pid)
            time.sleep(0.5)
            log.debug('process children: %s',[x for x in psutil.Process(proc.pid).children(recursive = True)])
            proc.communicate()
            log.debug('pull subprocess finished. return code: %s',proc.returncode)
            if proc.returncode:
                log.error('non-zero return code raising exception')
                raise subprocess.CalledProcessError(returncode =  proc.returncode, cmd = docker_pull_cmd)
        log.debug('moving on from pull')
    except RuntimeError as e:
        log.exception('caught RuntimeError')
        raise e
    except subprocess.CalledProcessError as exc:
        log.exception('subprocess failed. code: %s,  command %s',exc.returncode,exc.cmd)
        raise RuntimeError('failed docker subprocess in docker_enc_handler.')
    except:
        log.exception("Unexpected error: %s",sys.exc_info())
        raise
    finally:
        log.debug('finally for pull')
    
def docker_run(fullest_command,log,context,nametag):
    log.debug('docker run  command: \n%s',fullest_command)
    metadir = context['metadir']

    if 'PACKTIVITY_DRYRUN' in os.environ:
        return
    try:
        with open('{}/{}.run.log'.format(metadir,nametag),'w') as logfile:
            proc = subprocess.Popen(fullest_command,shell = True, stderr = subprocess.STDOUT, stdout = logfile)
            log.debug('started run subprocess with pid %s. now wait to finish',proc.pid)
            time.sleep(0.5)
            log.debug('process children: %s',[x for x in psutil.Process(proc.pid).children(recursive = True)])
            proc.communicate()
            log.debug('docker run subprocess finished. return code: %s',proc.returncode)
            if proc.returncode:
                log.error('non-zero return code raising exception')
                raise subprocess.CalledProcessError(returncode =  proc.returncode, cmd = fullest_command)
            log.debug('moving on from run')
    except subprocess.CalledProcessError as exc:
        log.exception('subprocess failed. code: %s,  command %s',exc.returncode,exc.cmd)
        raise RuntimeError('failed docker subprocess in docker_enc_handler.')
    except:
        log.exception("Unexpected error: %s",sys.exc_info())
        raise
    finally:
        log.debug('finally for run')

def mkdir_p(path):
    #http://stackoverflow.com/questions/600268/mkdir-p-functionality-in-python
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise

@environment('docker-encapsulated')
def docker_enc_handler(nametag,environment,context,job):
    log  = logging.getLogger('step_logger_{}'.format(nametag))
    log.setLevel(logging.DEBUG)
    metadir  = '{}/_packtivity'.format(context['readwrite'][0])
    context['metadir'] = metadir
    log.debug('creating metadirectory %s if necessary: %s',metadir,os.path.exists(metadir))
    mkdir_p(metadir)

    logname = '{}/{}.step.log'.format(metadir,nametag)
    fh  = logging.FileHandler(logname)
    fh.setLevel(logging.DEBUG)
    log.addHandler(fh)
    log.debug('starting log for step: %s',nametag)
    log.debug('context: \n %s',context)
        
    if 'PACKTIVITY_DOCKER_NOPULL' not in os.environ:
        docker_pull_cmd = 'docker pull {container}:{tag}'.format(
            container = environment['image'],
            tag = environment['imagetag']
        )
        docker_pull(docker_pull_cmd,log,context,nametag)
        
    docker_run_cmd = prepare_full_docker_cmd(nametag,context,environment,job['command'],log)
    docker_run(docker_run_cmd,log,context,nametag)
    log.debug('reached return for docker_enc_handler')
    
@environment('noop-env')
def noop_env(nametag,environment,context,job):
    log  = logging.getLogger('step_logger_{}'.format(nametag))
    log.info('context is: %s',context)
    log.info('would be running this job: %s',job)
    
@environment('localproc-env')
def localproc_env(nametag,environment,context,job):
    log  = logging.getLogger('step_logger_{}'.format(nametag))
    log.info('running local command %s',job['command'])
    try:
        subprocess.check_call(job['command'], shell = True)
    except:
        log.exception('local job failed. job: %s',job)