import os
import subprocess
import sys
import time
import psutil
import utils
import logging

handlers,environment = utils.handler_decorator()

def prepare_docker(nametag,context,do_cvmfs,do_grid,log):
    workdir  = context['workdir']
    metadir  = context['metadir']
    readonly = context.get('readonly',None)

    docker_mod = ''
    if 'PACKTIVITY_WORKDIR_LOCATION' not in os.environ:
        docker_mod += '-v {0}:{0}:rw'.format(os.path.abspath(workdir))
        if readonly:
            docker_mod += ' -v {0}:{0}:ro'.format(readonly)
    else:
        old,new = os.environ['PACKTIVITY_WORKDIR_LOCATION'].split(':')
        dockerpath = new+workdir.rsplit(old,1)[1]
        docker_mod += ' -v {0}:{1}:rw'.format(dockerpath,workdir)



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

    #return
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

@environment('docker-encapsulated')
def docker_enc_handler(nametag,environment,context,command):
    log  = logging.getLogger('step_logger_{}'.format(nametag))
    log.setLevel(logging.DEBUG)
    metadir  = '{}/_packtivity'.format(context['workdir'])
    context['metadir'] = metadir
    if not os.path.exists(metadir):
        os.makedirs(metadir)

    logname = '{}/{}.step.log'.format(metadir,nametag)
    fh  = logging.FileHandler(logname)
    fh.setLevel(logging.DEBUG)
    log.addHandler(fh)
    log.debug('starting log for step: %s',nametag)
    log.debug('context: \n %s',context)
    
    workdir = context['workdir']
    
    if 'PACKTIVITY_DOCKER_NOPULL' not in os.environ:
        docker_pull_cmd = 'docker pull {container}:{tag}'.format(
            container = environment['image'],
            tag = environment['imagetag']
        )
        docker_pull(docker_pull_cmd,log,context,nametag)

    docker_run_cmd = prepare_full_docker_cmd(nametag,context,environment,command,log)
    docker_run(docker_run_cmd,log,context,nametag)
    log.debug('reached return for docker_enc_handler')
    
@environment('noop-env')
def noop_env(nametag,environment,context,command):
    log  = logging.getLogger('step_logger_{}'.format(nametag))
    log.info('context is: %s',context)
    log.info('would be running this command: %s',command)
