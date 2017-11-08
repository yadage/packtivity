import os
import subprocess
import sys
import time
import psutil
import shlex
import pipes

import click
import yaml

import packtivity.utils as utils
import packtivity.logutils as logutils

handlers,executor = utils.handler_decorator()

def sourcepath(path):
    if 'PACKTIVITY_WORKDIR_LOCATION' in os.environ:
        old,new = os.environ['PACKTIVITY_WORKDIR_LOCATION'].split(':')
        dockerpath = new+path.rsplit(old,1)[1]
        return dockerpath
    else:
        return path

def prepare_par_mounts(parmounts,state):
    mounts = []
    for i,x in enumerate(parmounts):
        parmountfile = os.path.join(state.readwrite[0],'_yadage_parmount_{}.txt'.format(i))
        with open(parmountfile,'w') as f:
            f.write(x['mountcontent'])

        mounts.append('{}:{}'.format(
            os.path.abspath(parmountfile),
            x['mountpath']
        ))

    return mounts

def state_context_to_mounts(state):
    readwrites  = state.readwrite
    readonlies = state.readonly
    mounts = ''
    for rw in readwrites:
        mounts += '-v {}:{}:rw'.format(sourcepath(os.path.abspath(rw)),rw)
    for ro in readonlies:
        mounts += ' -v {}:{}:ro'.format(sourcepath(ro),ro)
    return mounts

def cvmfs_from_volume_plugin(command_line,cvmfs_repos = None):
    if not cvmfs_repos:
        cvmfs_repos = yaml.load(os.environ.get('PACKTIVITY_CVMFS_REPOS','null'))
    if not cvmfs_repos:
        cvmfs_repos  = ['atlas.cern.ch','atlas-condb.cern.ch','sft.cern.ch']
    command_line += ' --security-opt label:disable'
    for repo in cvmfs_repos:
        command_line += ' --volume-driver cvmfs -v {cvmfs_repo}:/cvmfs/{cvmfs_repo}'.format(cvmfs_repo = repo)
    return command_line

def cvmfs_from_external_mount(command_line):
    command_line+=' -v {}:/cvmfs'.format(os.environ.get('PACKTIVITY_CVMFS_LOCATION','/cvmfs'))
    return command_line

def prepare_docker(state,do_cvmfs,do_auth,par_mounts,log,metadata):
    docker_mod = state_context_to_mounts(state)



    if do_cvmfs:
        cvmfs_source = os.environ.get('PACKTIVITY_CVMFS_SOURCE','external')
        if cvmfs_source == 'external':
            docker_mod = cvmfs_from_external_mount(docker_mod)
        elif cvmfs_source == 'voldriver':
            docker_mod = cvmfs_from_volume_plugin(docker_mod)
        else:
            raise RuntimeError('unknown CVMFS location requested')

    if do_auth:
        if 'PACKTIVITY_AUTH_LOCATION' not in os.environ:
            docker_mod+=' -v /home/recast/recast_auth:/recast_auth'
        else:
            docker_mod+=' -v {}:/recast_auth'.format(os.environ['PACKTIVITY_AUTH_LOCATION'])


    for x in par_mounts:
        docker_mod+=' -v {}'.format(x)


    cidfile = '{}/{}.cid'.format(state.metadir,metadata['name'])

    if os.path.exists(cidfile):
        log.warning('cid file %s seems to exist, container execution will crash',cidfile)
    docker_mod += ' --cidfile {}'.format(cidfile)


    docker_mod += ' {}'.format(os.environ.get('PACKTIVITY_DOCKER_CMD_MOD',''))

    return docker_mod

def prepare_docker_context(state,environment,log,metadata):
    report = '''\n\
--------------
run in docker container image: {image}
with env: {env}
resources: {resources}
--------------
    '''.format(image = environment['image'],
               env = environment['envscript'] if environment['envscript'] else 'default env',
               resources = environment['resources']
              )
    log.debug(report)

    do_cvmfs = 'CVMFS' in environment['resources']
    do_auth  = ('GRIDProxy'  in environment['resources']) or ('KRB5Auth' in environment['resources'])
    log.debug('do_auth: %s do_cvmfs: %s',do_auth,do_cvmfs)

    par_mounts = prepare_par_mounts(environment['par_mounts'], state)

    return prepare_docker(state,do_cvmfs,do_auth,par_mounts,log,metadata)

def run_docker_with_script(state,environment,job,log,metadata):
    script = job['script']
    interpreter = job['interpreter']

    log.debug('script is:')
    log.debug('\n--------------\n'+script+'\n--------------')
    if 'PACKTIVITY_DRYRUN' in os.environ:
        return

    indocker = interpreter
    envmod = 'source {} && '.format(environment['envscript']) if environment['envscript'] else ''
    indocker = envmod+indocker

    try:
        with logutils.setup_logging_topic(metadata,state,'run', return_logger = True) as runlog:
            subcmd = docker_execution_cmdline(
                state,environment,log,metadata,
                combined_flags = '--rm -i',
                cmd_argv = ['sh', '-c', indocker]
            )
            log.debug('running docker cmd: %s',subcmd)
            proc = subprocess.Popen(shlex.split(subcmd), stdin = subprocess.PIPE, stderr = subprocess.STDOUT, stdout = subprocess.PIPE, bufsize=1, close_fds = True)

            log.debug('started run subprocess with pid %s. now piping script',proc.pid)
            proc.stdin.write(script.encode('utf-8'))
            proc.stdin.close()
            time.sleep(0.5)

            for line in iter(proc.stdout.readline, b''):
                runlog.info(line.strip())
            while proc.poll() is None:
                pass

            proc.stdout.close()

        log.debug('container execution finished. return code: %s',proc.returncode)
        if proc.returncode:
            log.error('non-zero return code raising exception')
            raise subprocess.CalledProcessError(returncode =  proc.returncode, cmd = subcmd)
        log.debug('moving on from run')
    except subprocess.CalledProcessError as exc:
        log.exception('subprocess failed. code: %s,  command %s',exc.returncode,exc.cmd)
        raise RuntimeError('failed container execution.')
    except:
        log.exception("Unexpected error: %s",sys.exc_info())
        raise
    finally:

        log.debug('finally for run')

def docker_execution_cmdline(state,environment,log,metadata,combined_flags,cmd_argv):
    quoted_string = ' '.join(map(pipes.quote,cmd_argv))

    image = environment['image']
    imagetag = environment['imagetag']
    docker_mod = prepare_docker_context(state,environment,log,metadata)
    workdir_flag =  '-w {}'.format(environment['workdir']) if environment['workdir'] is not None else ''

    return 'docker run {} {} {} {}:{} {}'.format(
        combined_flags,
        workdir_flag,
        docker_mod,
        image,
        imagetag,
        quoted_string
    )

def run_docker_with_oneliner(state,environment,command,log,metadata):
    log.debug('''\n\
--------------
running one liner in container.
command: {command}
--------------
    '''.format(command = command))

    envmod = 'source {} &&'.format(environment['envscript']) if environment['envscript'] else ''
    in_docker_cmd = '{envmodifier} {command}'.format(envmodifier = envmod, command = command)

    docker_run_cmd_str = docker_execution_cmdline(
        state,environment,log,metadata,
        combined_flags = '--rm',
        cmd_argv = ['sh', '-c', in_docker_cmd]
    )
    docker_run_cmd(docker_run_cmd_str,log,state,metadata)

def docker_pull(docker_pull_cmd,log,state,metadata):
    log.debug('container image pull command: \n  %s',docker_pull_cmd)
    if 'PACKTIVITY_DRYRUN' in os.environ:
        return
    try:
        with logutils.setup_logging_topic(metadata,state,'pull', return_logger = True) as pulllog:
            proc = subprocess.Popen(shlex.split(docker_pull_cmd), stderr = subprocess.STDOUT, stdout = subprocess.PIPE, bufsize=1, close_fds = True)
            log.debug('started pull subprocess with pid %s. now wait to finish',proc.pid)
            time.sleep(0.5)
            log.debug('process children: %s',[x for x in psutil.Process(proc.pid).children(recursive = True)])

            for line in iter(proc.stdout.readline, b''):
                pulllog.info(line.strip())
            while proc.poll() is None:
                pass

            proc.stdout.close()

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
        raise RuntimeError('failed container image pull subprocess in docker_enc_handler.')
    except:
        log.exception("Unexpected error: %s",sys.exc_info())
        raise
    finally:
        log.debug('finally for pull')

def docker_run_cmd(fullest_command,log,state,metadata):
    log.debug('container execution command: \n%s',fullest_command)
    if 'PACKTIVITY_DRYRUN' in os.environ:
        return
    try:
        with logutils.setup_logging_topic(metadata,state,'run', return_logger = True) as runlog:
            proc = subprocess.Popen(shlex.split(fullest_command), stderr = subprocess.STDOUT, stdout = subprocess.PIPE, bufsize=1, close_fds = True)
            log.debug('started run subprocess with pid %s. now wait to finish',proc.pid)
            time.sleep(0.5)
            log.debug('process children: %s',[x for x in psutil.Process(proc.pid).children(recursive = True)])

            for line in iter(proc.stdout.readline, b''):
                runlog.info(line.strip())
            while proc.poll() is None:
                pass

            proc.stdout.close()

        log.debug('container execution subprocess finished. return code: %s',proc.returncode)
        if proc.returncode:
            log.error('non-zero return code raising exception')
            raise subprocess.CalledProcessError(returncode =  proc.returncode, cmd = fullest_command)
        log.debug('moving on from run')
    except subprocess.CalledProcessError as exc:
        log.exception('subprocess failed. code: %s,  command %s',exc.returncode,exc.cmd)
        raise RuntimeError('failed container execution subprocess.')
    except:
        log.exception("Unexpected error: %s",sys.exc_info())
        raise
    finally:
        log.debug('finally for run')




@executor('docker-encapsulated')
def docker_enc_handler(environment,state,job,metadata):
    with logutils.setup_logging_topic(metadata,state,'step',return_logger = True) as log:
        log.debug('starting log for step: %s',metadata)
        if 'PACKTIVITY_DOCKER_NOPULL' not in os.environ:
            log.info('prepare pull')
            docker_pull_cmd = 'docker pull {container}:{tag}'.format(
                container = environment['image'],
                tag = environment['imagetag']
            )
            docker_pull(docker_pull_cmd,log,state,metadata)

        log.info('running job')

        if 'command' in job:
            run_docker_with_oneliner(state,environment,job['command'],log,metadata)
            log.debug('reached return for docker_enc_handler')
        elif 'script' in job:
            run_docker_with_script(state,environment,job,log,metadata)
        else:
            raise RuntimeError('do not know yet how to run this...')

@executor('noop-env')
def noop_env(environment,state,job,metadata):
    with logutils.setup_logging_topic(metadata,state,'step',return_logger = True) as log:
        log.info('state is: %s',state)
        log.info('would be running this job: %s',job)

@executor('localproc-env')
def localproc_env(environment,state,job,metadata):
    with logutils.setup_logging_topic(metadata,state,'step',return_logger = True) as log:
        olddir = os.path.realpath(os.curdir)
        workdir = state.readwrite[0]
        try:
            log.info('changing to workdirectory %s',workdir)
            utils.mkdir_p(workdir)
            os.chdir(workdir)
            shell = ['sh', '-c', str(job['command'])]
            # shell = ['sh','-c','echo hello world']
            log.info('running %s', shell)
            subprocess.check_call(shell)
        except:
            log.exception('local job failed. job: %s',job)
            raise RuntimeError('failed')
        finally:
            log.info('changing back to original directory %s',olddir)
            os.chdir(olddir)

@executor('manual-env')
def manual_env(environment,state,job,metadata):
    instructions = environment['instructions']
    ctx = yaml.safe_dump(state,default_flow_style = False)
    click.secho(instructions, fg = 'blue')
    click.secho(ctx, fg = 'cyan')

@executor('test-env')
def test_process(environment,state,job,metadata):
    with logutils.setup_logging_topic(metadata,state,'step',return_logger = True) as log:
        log.info('a complicated test environment')
        log.info('job:  {}'.format(job))
        log.info('env:  {}'.format(environment))
        log.info('state {}'.format(state))
