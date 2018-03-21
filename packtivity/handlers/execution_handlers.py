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

def state_context_to_mounts(state):
    readwrites  = state.readwrite
    readonlies = state.readonly

    mounts = []
    for rw in readwrites:
        mounts.append({
            'type': 'bind',
            'source': sourcepath(os.path.abspath(rw)),
            'destination': rw,
            'readonly': False
        })
    for ro in readonlies:
        mounts.append({
            'type': 'bind',
            'source': sourcepath(os.path.abspath(ro)),
            'destination': ro,
            'readonly': False
        })
    return mounts

def prepare_par_mounts(parmounts,state):
    mounts = []
    for i,x in enumerate(parmounts):
        parmountfile = os.path.join(state.readwrite[0],'_yadage_parmount_{}.txt'.format(i))
        with open(parmountfile,'w') as f:
            f.write(x['mountcontent'])

        mounts.append({
            'type': 'bind',
            'source': sourcepath(os.path.abspath(parmountfile)),
            'destination': x['mountpath'],
            'readonly': False
        })

    return mounts

def cvmfs_from_volume_plugin(cvmfs_repos = None):
    if not cvmfs_repos:
        cvmfs_repos = yaml.load(os.environ.get('PACKTIVITY_CVMFS_REPOS','null'))
    if not cvmfs_repos:
        cvmfs_repos  = ['atlas.cern.ch','atlas-condb.cern.ch','sft.cern.ch']

    options = '--security-opt label:disable'
    mounts = []
    for repo in cvmfs_repos:
        mounts.append({
            'type': 'volume',
            'source': repo,
            'destination': '/cvmfs/{}'.format(repo),
            'readonly': False
        })

    return options, mounts

def cvmfs_from_external_mount():
    return '', [{
        'type': 'volume',
        'source': os.environ.get('PACKTIVITY_CVMFS_LOCATION','/cvmfs'),
        'destination': '/cvmfs',
        'readonly': False
    }]

def cvmfs_mount():
    cvmfs_source = os.environ.get('PACKTIVITY_CVMFS_SOURCE','external')
    if cvmfs_source == 'external':
        return cvmfs_from_external_mount()
    elif cvmfs_source == 'voldriver':
        return cvmfs_from_volume_plugin()
    else:
        raise RuntimeError('unknown CVMFS location requested')

def auth_mount():
    return [{
        'type': 'bind',
        'source': os.environ.get('PACKTIVITY_AUTH_LOCATION','/home/recast/recast_auth'),
        'destination': '/recast_auth',
        'readonly': False
    }]

def resource_mounts(state,environment,log):
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

    options, mounts = '', []

    if do_cvmfs:
        cvfms_options, cvmfs_mounts = cvmfs_mount()
        options += cvfms_options
        mounts  += cvmfs_mounts
    if do_auth:
        mounts  += auth_mount()

    return options, mounts

def docker_execution_cmdline(state,environment,log,metadata,stdin,cmd_argv):
    quoted_string = ' '.join(map(pipes.quote,cmd_argv))

    image = environment['image']
    imagetag = environment['imagetag']

    # generic non-volume mount flags
    workdir_flag =  '-w {}'.format(environment['workdir']) if environment['workdir'] is not None else ''

    cidfile = '{}/{}.cid'.format(state.metadir,metadata['name'])
    if os.path.exists(cidfile):
        log.warning('cid file %s seems to exist, container execution will crash',cidfile)
    cid_file = '--cidfile {}'.format(cidfile)

    custom_mod = ' {}'.format(os.environ.get('PACKTIVITY_DOCKER_CMD_MOD',''))

    # volume mounts (resources, parameter mounts and state mounts)
    state_mounts = state_context_to_mounts(state)
    par_mounts   = prepare_par_mounts(environment['par_mounts'], state)
    rsrcs_opts, rsrcs_mounts = resource_mounts(state,environment,log)


    mount_args = ''
    for s in state_mounts + par_mounts + rsrcs_mounts:
        mount_args += ' -v {source}:{destination}:{mode}'.format(
            source = s['source'],
            destination = s['destination'],
            mode = 'ro' if s['readonly'] else 'rw'
        )

    return 'docker run --rm {stdin} {cid} {workdir} {custom} {mount_args} {rsrcs_opts} {img}:{tag} {command}'.format(
        stdin = '-i' if stdin else '',
        cid = cid_file,
        workdir = workdir_flag,
        custom = custom_mod,
        mount_args = mount_args,
        rsrcs_opts = rsrcs_opts,
        img = image,
        tag = imagetag,
        command = quoted_string
    )

def run_docker_with_script(environment,job,log):
    script = job['script']
    interpreter = job['interpreter']

    log.debug('script is:')
    log.debug('\n--------------\n'+script+'\n--------------')

    indocker = interpreter
    envmod = 'source {} && '.format(environment['envscript']) if environment['envscript'] else ''
    in_docker_cmd = envmod+indocker
    return ['sh', '-c', in_docker_cmd], script

def run_docker_with_oneliner(environment,job,log):
    log.debug('''\n\
--------------
running one liner in container.
command: {command}
--------------
    '''.format(command = job['command']))

    envmod = 'source {} && '.format(environment['envscript']) if environment['envscript'] else ''
    in_docker_cmd = envmod + job['command']
    return ['sh', '-c', in_docker_cmd], None

def execute_docker(metadata,state,log,docker_run_cmd_str,stdin_content = None):
    log.debug('container execution command: \n%s',docker_run_cmd_str)
    log.debug('stdin if any: %s', stdin_content)
    if 'PACKTIVITY_DRYRUN' in os.environ:
        return
    try:
        with logutils.setup_logging_topic(metadata,state,'run', return_logger = True) as runlog:

            proc = None
            if stdin_content:
                log.debug('stdin: \n%s',stdin_content)
                proc = subprocess.Popen(shlex.split(docker_run_cmd_str),
                                        stdin = subprocess.PIPE,
                                        stderr = subprocess.STDOUT,
                                        stdout = subprocess.PIPE,
                                        bufsize=1,
                                        close_fds = True)
                proc.stdin.write(stdin_content.encode('utf-8'))
                proc.stdin.close()
            else:
                proc = subprocess.Popen(shlex.split(docker_run_cmd_str), stderr = subprocess.STDOUT, stdout = subprocess.PIPE, bufsize=1, close_fds = True)

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
            raise subprocess.CalledProcessError(returncode =  proc.returncode, cmd = docker_run_cmd_str)
        log.debug('moving on from run')
    except subprocess.CalledProcessError as exc:
        log.exception('subprocess failed. code: %s,  command %s',exc.returncode,exc.cmd)
        raise RuntimeError('failed container execution subprocess.')
    except:
        log.exception("Unexpected error: %s",sys.exc_info())
        raise
    finally:
        log.debug('finally for run')

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


@executor('docker-encapsulated')
def docker_enc_handler(environment,state,job,metadata):
    with logutils.setup_logging_topic(metadata,state,'step',return_logger = True) as log:
        if 'command' in job:
            stdin = False
            container_argv, container_stdin = run_docker_with_oneliner(environment,job,log)
        elif 'script' in job:
            stdin = True
            container_argv, container_stdin = run_docker_with_script(environment,job,log)
        else:
            raise RuntimeError('do not know yet how to run this...')

        cmdline = docker_execution_cmdline(
            state,environment,log,metadata,
            stdin = stdin,
            cmd_argv = container_argv
        )

        if 'PACKTIVITY_DOCKER_NOPULL' not in os.environ:
            log.info('prepare pull')
            docker_pull_cmd = 'docker pull {container}:{tag}'.format(
                container = environment['image'],
                tag = environment['imagetag']
            )
            docker_pull(docker_pull_cmd,log,state,metadata)
        execute_docker(metadata,state,log,cmdline, stdin_content = container_stdin)

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
