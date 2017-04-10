import tempfile

import os
import subprocess
import sys
import packtivity.utils as utils
import time
import psutil
import logging
import click
import yaml
import shlex

handlers,environment = utils.handler_decorator()

def sourcepath(path):
    if 'PACKTIVITY_WORKDIR_LOCATION' in os.environ:
        old,new = os.environ['PACKTIVITY_WORKDIR_LOCATION'].split(':')
        dockerpath = new+path.rsplit(old,1)[1]
        return dockerpath
    else:
        return path

def prepare_docker(context,do_cvmfs,do_auth,log):
    nametag = context['nametag']
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
            docker_mod+=' -v {}:/cvmfs'.format(os.environ['PACKTIVITY_CVMFS_LOCATION'])
    if do_auth:
        if 'PACKTIVITY_AUTH_LOCATION' not in os.environ:
            docker_mod+=' -v /home/recast/recast_auth:/recast_auth'
        else:
            docker_mod+=' -v {}:/recast_auth'.format(os.environ['PACKTIVITY_AUTH_LOCATION'])

    cidfile = '{}/{}.cid'.format(metadir,nametag)

    if os.path.exists(cidfile):
        log.warning('cid file %s seems to exist, docker run will crash',cidfile)
    docker_mod += ' --cidfile {}'.format(cidfile)

    return docker_mod


def prepare_docker_context(context,environment,log):
    container = environment['image']
    report = '''\n\
--------------
run in docker container: {container}
with env: {env}
resources: {resources}
--------------
    '''.format(container = container,
               env = environment['envscript'] if environment['envscript'] else 'default env',
               resources = environment['resources']
              )
    log.debug(report)

    do_cvmfs = 'CVMFS' in environment['resources']
    do_auth  = ('GRIDProxy'  in environment['resources']) or ('KRB5Auth' in environment['resources'])
    log.debug('do_auth: %s do_cvmfs: %s',do_auth,do_cvmfs)



    docker_mod = prepare_docker(context,do_cvmfs,do_auth,log)
    return docker_mod

def run_docker_with_script(context,environment,job,log):
    metadir  = context['metadir']
    image = environment['image']
    imagetag = environment['imagetag']
    nametag = context['nametag']

    script = job['script']
    interpreter = job['interpreter']

    do_cvmfs = 'CVMFS' in environment['resources']
    log.debug('script is:')
    log.debug('\n--------------\n'+script+'\n--------------')
    docker_mod = prepare_docker_context(context,environment,log)
    if 'PACKTIVITY_DRYRUN' in os.environ:
        return

    indocker = interpreter
    envmod = 'source {} && '.format(environment['envscript']) if environment['envscript'] else ''
    indocker = envmod+indocker

    try:
        with open('{}/{}.run.log'.format(metadir,nametag),'w') as logfile:
            if do_cvmfs:
                if 'PACKTIVITY_WITHIN_DOCKER' not in os.environ:
                    subprocess.check_call('cvmfs_config probe')

            subcmd = 'docker run --rm -i {docker_mod} {image}:{imagetag} sh -c \'{indocker}\' '.format(image = image, imagetag = imagetag, docker_mod = docker_mod, indocker = indocker)
            log.debug('running docker cmd: %s',subcmd)
            proc = subprocess.Popen(shlex.split(subcmd), stdin = subprocess.PIPE, stderr = subprocess.STDOUT, stdout = logfile)
            log.debug('started run subprocess with pid %s. now piping script',proc.pid)
            proc.communicate(script)
            log.debug('docker run subprocess finished. return code: %s',proc.returncode)
            if proc.returncode:
                log.error('non-zero return code raising exception')
                raise subprocess.CalledProcessError(returncode =  proc.returncode, cmd = subcmd)
            log.debug('moving on from run')
    except subprocess.CalledProcessError as exc:
        log.exception('subprocess failed. code: %s,  command %s',exc.returncode,exc.cmd)
        raise RuntimeError('failed docker run subprocess in docker_enc_handler.')
    except:
        log.exception("Unexpected error: %s",sys.exc_info())
        raise
    finally:
        log.debug('finally for run')

def prepare_full_docker_with_oneliner(context,environment,command,log):
    image = environment['image']
    imagetag = environment['imagetag']
    do_cvmfs = 'CVMFS' in environment['resources']

    report = '''\n\
--------------
running one liner in container.
command: {command}
--------------
    '''.format(command = command)
    log.debug(report)

    docker_mod = prepare_docker_context(context,environment,log)

    envmod = 'source {} &&'.format(environment['envscript']) if environment['envscript'] else ''
    in_docker_cmd = '{envmodifier} {command}'.format(envmodifier = envmod, command = command)

    fullest_command = 'docker run --rm {docker_mod} {image}:{imagetag} sh -c \'{in_dock}\''.format(
                        docker_mod = docker_mod,
                        image = image,
                        imagetag = imagetag,
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
            proc = subprocess.Popen(shlex.split(docker_pull_cmd), stderr = subprocess.STDOUT, stdout = logfile)
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
        raise RuntimeError('failed docker pull subprocess in docker_enc_handler.')
    except:
        log.exception("Unexpected error: %s",sys.exc_info())
        raise
    finally:
        log.debug('finally for pull')

def docker_run_cmd(fullest_command,log,context,nametag):
    log.debug('docker run  command: \n%s',fullest_command)
    metadir = context['metadir']

    if 'PACKTIVITY_DRYRUN' in os.environ:
        return
    try:
        with open('{}/{}.run.log'.format(metadir,nametag),'w') as logfile:
            proc = subprocess.Popen(shlex.split(fullest_command), stderr = subprocess.STDOUT, stdout = logfile)
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
        raise RuntimeError('failed docker run subprocess in docker_enc_handler.')
    except:
        log.exception("Unexpected error: %s",sys.exc_info())
        raise
    finally:
        log.debug('finally for run')


def remove_docker_image(image, log_filename, logger):
    with open(log_filename, 'w') as logfile:
        try:
            p = subprocess.Popen(
                ["docker", "rmi", image],
                stdout=logfile,
                stderr=subprocess.STDOUT
            )
            logger.debug("docker rmi process PID: %s" % p.pid)
            p.communicate()
            returncode = p.returncode
            if returncode != 0:
                logger.error("Docker execution failed, return code %s" % returncode)
                raise RuntimeError("Docker execution failed, return code %s" % returncode)
            logger.debug("docker rmi command completed successfully")
        except (OSError, IOError) as e:
            logger.exception("subprocess failed: %s", sys.exc_info())
            raise RuntimeError("docker rmi execution failed, subprocess error")

@environment('tarball')
def tarball_handler(environment, context, job):

    url = environment['url']
    image = environment['image']
    nametag = context['nametag']

    # prepare logging for the execution of the job. We're ready to handle up to DEBUG
    log = logging.getLogger('step_logger_{}'.format(url))
    log.setLevel(logging.DEBUG)

    # This is all internal loggin, we don't want to escalate to handlers of parent loggers
    # we will have two handlers, a stream handler logging to stdout at INFO
    log.propagate = False
    fmt = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
    fh = logging.StreamHandler()
    fh.setLevel(logging.INFO)
    fh.setFormatter(fmt)
    log.addHandler(fh)

    # short interruption to create metainfo storage location
    metadir = '{}/_packtivity'.format(context['readwrite'][0])
    context['metadir'] = metadir

    if not os.path.exists(metadir):
        log.info('Creating metadirectory %s', metadir)
        utils.mkdir_p(metadir)

    # Now that we have  place to store meta information we put a file based logger in place
    # to log at DEBUG
    logname = '{}/{}.step.log'.format(metadir, nametag)
    fh = logging.FileHandler(logname)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    log.addHandler(fh)
    log.debug('starting log for step: %s', nametag)
    log.debug('context: %s', context)
    log.info('Executing docker import')
    with open(os.path.join(metadir, '%s.docker.import.log' % nametag), 'w') as logfile:
        try:
            p = subprocess.Popen(
                ["docker", "import", url, image],
                stdout=logfile,
                stderr=subprocess.STDOUT
            )
            log.debug("Docker process PID: %s" % p.pid)
            p.communicate()
            returncode = p.returncode
            if returncode != 0:
                log.error("Docker execution failed, return code %s" % returncode)
                raise RuntimeError("Docker execution failed, return code %s" % returncode)
            log.debug("docker import command completed successfully")
        except (OSError, IOError) as e:
            log.exception("subprocess failed: %s", sys.exc_info())
            raise RuntimeError("Docker execution failed, subprocess error")

    if 'command' in job:
        # log.info('running oneliner command')
        docker_run_cmd_str = prepare_full_docker_with_oneliner(context, environment, job['command'], log)
        docker_run_cmd(docker_run_cmd_str, log, context, nametag)
        log.debug('reached return for docker_enc_handler')
    elif 'script' in job:
        run_docker_with_script(context,environment, job, log)
    else:
        remove_docker_image(image=image, log_filename=os.path.join(metadir, '%s.docker.rmi.log' % nametag), logger=log)
        raise RuntimeError('do not know yet how to run this...')
    remove_docker_image(image=image, log_filename=os.path.join(metadir, '%s.docker.rmi.log' % nametag), logger=log)


@environment('umbrella')
def umbrella(environment, context, job):
    metadir = '{}/_packtivity'.format(context['readwrite'][0])
    context['metadir'] = metadir

    if not os.path.exists(metadir):
        utils.mkdir_p(metadir)
    fp = open(os.path.join(metadir, "umbrella_output.txt"), "w")
    specification_file = environment['spec_url']
    command = job.get('command', None)
    tempdir = tempfile.mkdtemp()
    logfile = job.get('logfile', 'umbrella.log')
    # docker_mod = prepare_docker(context=context, do_cvmfs=False, do_auth=False, log=logfile)

    readwrites  = context['readwrite']
    readonlies = context['readonly']
    options = [
                    "umbrella",
                     "--spec", specification_file,
                     "--sandbox_mode", "docker",
                     '--localdir', tempdir,
                     '--log', logfile,
     ]
    volumes = ""
    for item in readwrites:
        # Umbrella cuts off last character in path
        if item[-1] != "/":
            item = item + "/"
        item = item + "=" + item
        volumes += item + ","

    for item in readonlies:
        # Umbrella cuts off last character in path
        if item[-1] != "/":
            item = item + "/"
        item = item + "=" + item
        volumes += item + ","

    if volumes:
        # Remove the trailing comma
        volumes = volumes[:-1]
        options.append("-i")
        options.append(volumes)
    options.append('run')
    options.append(command)
    print options
    try:
        if not command:
            command = job.get('script', None)
        if not command:
            raise RuntimeError('command or script option must be provided')
        try:
            p = subprocess.Popen(
                options,
                stdout=fp,
                stderr=fp,
                # stdout=subprocess.STDOUT,
                # stderr=subprocess.STDOUT
            )
            # log.debug("Umbrella process PID: %s" % p.pid)
            p.communicate()
            returncode = p.returncode
            if returncode != 0:
                # log.error("Docker execution failed, return code %s" % returncode)
                raise RuntimeError("Umbrella execution failed, return code %s" % returncode)
            # log.debug("docker import command completed successfully")
        except (OSError, IOError) as e:
            # log.exception("subprocess failed: %s", sys.exc_info())
            raise RuntimeError("Umbrella execution failed, subprocess error (%s)" % e)

    except Exception as e:
        # Clean up
        os.rmdir(tempdir)
        raise e

@environment('docker-encapsulated')
def docker_enc_handler(environment,context,job):
    nametag = context['nametag']

    ## prepare logging for the execution of the job. We're ready to handle up to DEBUG
    log  = logging.getLogger('step_logger_{}'.format(nametag))
    log.setLevel(logging.DEBUG)

    ## this is all internal loggin, we don't want to escalate to handlers of parent loggers
    ## we will have two handlers, a stream handler logging to stdout at INFO
    log.propagate = False
    fmt = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
    fh  = logging.StreamHandler()
    fh.setLevel(logging.INFO)
    fh.setFormatter(fmt)
    log.addHandler(fh)

    # short interruption to create metainfo storage location
    metadir  = '{}/_packtivity'.format(context['readwrite'][0])
    context['metadir'] = metadir

    log.info('creating metadirectory %s if necessary. exists? : %s',metadir,os.path.exists(metadir))
    utils.mkdir_p(metadir)

    # Now that we have  place to store meta information we put a file based logger in place
    # to log at DEBUG
    logname = '{}/{}.step.log'.format(metadir,nametag)
    fh  = logging.FileHandler(logname)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    log.addHandler(fh)
    log.debug('starting log for step: %s',nametag)
    if 'PACKTIVITY_DOCKER_NOPULL' not in os.environ:
        log.info('prepare pull')
        docker_pull_cmd = 'docker pull {container}:{tag}'.format(
            container = environment['image'],
            tag = environment['imagetag']
        )
        docker_pull(docker_pull_cmd,log,context,nametag)

    log.info('running job')

    if 'command' in job:
        # log.info('running oneliner command')
        docker_run_cmd_str = prepare_full_docker_with_oneliner(context,environment,job['command'],log)
        docker_run_cmd(docker_run_cmd_str,log,context,nametag)
        log.debug('reached return for docker_enc_handler')
    elif 'script' in job:
        run_docker_with_script(context,environment,job,log)
    else:
        raise RuntimeError('do not know yet how to run this...')

@environment('noop-env')
def noop_env(environment,context,job):
    nametag = context['nametag']
    log  = logging.getLogger('step_logger_{}'.format(nametag))
    log.info('context is: %s',context)
    log.info('would be running this job: %s',job)

@environment('localproc-env')
def localproc_env(environment,context,job):
    nametag = context['nametag']
    log  = logging.getLogger('step_logger_{}'.format(nametag))
    olddir = os.path.realpath(os.curdir)
    workdir = context['readwrite'][0]
    log.info('running local command %s',job['command'])
    try:
        log.info('changing to workdirectory %s',workdir)
        utils.mkdir_p(workdir)
        os.chdir(workdir)
        #this is used for testing and we will keep this shell
        #doesn't make sense to wrap in sh ...
        subprocess.check_call(job['command'], shell = True)
    except:
        log.exception('local job failed. job: %s',job)
    finally:
        log.info('changing back to original directory %s',olddir)
        os.chdir(olddir)

@environment('manual-env')
def manual_env(environment,context,job):
    instructions = environment['instructions']
    ctx = yaml.safe_dump(context,default_flow_style = False)
    click.secho(instructions, fg = 'blue')
    click.secho(ctx, fg = 'cyan')

@environment('pathena-submit-env')
def pathena_submit_env(environment,context,job):
    import grid_handlers
    return grid_handlers.execute_grid_job(environment,context,job)
