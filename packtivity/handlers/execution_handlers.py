import os
import subprocess
import sys
import time
import psutil
import shlex
import pipes

import click
import yaml
import json

import packtivity.utils as utils
import packtivity.logutils as logutils

handlers,executor = utils.handler_decorator()

def sourcepath(config,path):
    workdir_location = config.container_config.workdir_location()
    if workdir_location:
        old,new = workdir_location.split(':')
        dockerpath = new+path.rsplit(old,1)[1]
        return dockerpath
    else:
        return path

def state_context_to_mounts(config,state):
    readwrites  = state.readwrite
    readonlies = state.readonly

    mounts = []
    for rw in readwrites:
        mounts.append({
            'type': 'bind',
            'source': sourcepath(config,os.path.abspath(rw)),
            'destination': rw,
            'readonly': False
        })
    for ro in readonlies:
        mounts.append({
            'type': 'bind',
            'source': sourcepath(config,os.path.abspath(ro)),
            'destination': ro,
            'readonly': False
        })
    return mounts

def prepare_par_mounts(config,parmounts,state):
    mounts = []
    for i,x in enumerate(parmounts):
        parmountfile = os.path.join(state.readwrite[0],'_yadage_parmount_{}.txt'.format(i))
        with open(parmountfile,'w') as f:
            f.write(x['mountcontent'])

        mounts.append({
            'type': 'bind',
            'source': sourcepath(config,os.path.abspath(parmountfile)),
            'destination': x['mountpath'],
            'readonly': False
        })

    return mounts

def cvmfs_from_volume_plugin(config,cvmfs_repos = None):
    cvmfs_repos = config.container_config.cvmfs_repos()

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

def cvmfs_from_external_mount(config):
    return '', [{
        'type': 'bind',
        'source': config.container_config.cvmfs_location(),
        'destination': '/cvmfs',
        'readonly': False
    }]

def cvmfs_mount(config):
    cvmfs_source = config.container_config.cvmfs_source()
    if cvmfs_source == 'external':
        return cvmfs_from_external_mount(config)
    elif cvmfs_source == 'voldriver':
        return cvmfs_from_volume_plugin(config)
    else:
        raise RuntimeError('unknown CVMFS location requested')

def auth_mount(config):
    return [{
        'type': 'bind',
        'source': config.container_config.auth_location(),
        'destination': '/recast_auth',
        'readonly': False
    }]

def resource_mounts(config,state,environment,log):
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
        cvfms_options, cvmfs_mounts = cvmfs_mount(config)
        options += cvfms_options
        mounts  += cvmfs_mounts
    if do_auth:
        mounts  += auth_mount(config)

    return options, mounts

def docker_execution_cmdline(config,state,log,metadata, race_spec):

    #docker specific container id
    cidfile = '{}/{}.cid'.format(state.metadir,metadata['name'])
    if os.path.exists(cidfile):
        log.warning('cid file %s seems to exist, container execution will crash',cidfile)
    cid_file = '--cidfile {}'.format(cidfile)

    #docker specific execution modifier
    custom_mod = ' {}'.format(config.container_config.container_runtime_modifier())

    #for running in subprocess
    quoted_string = ' '.join(map(pipes.quote,race_spec['argv']))

    # generic non-volume mount flags
    workdir_flag =  '-w {}'.format(race_spec['workdir']) if race_spec['workdir'] is not None else ''
    mount_args = ''
    for s in race_spec['mounts']:
        mount_args += ' -v {source}:{destination}:{mode}'.format(
            source = s['source'],
            destination = s['destination'],
            mode = 'ro' if s['readonly'] else 'rw'
        )

    return 'docker run --rm {stdin} {cid} {workdir} {custom} {mount_args} {image} {command}'.format(
        stdin = '-i' if race_spec['stdin'] else '',
        cid        = cid_file,
        workdir    = workdir_flag,
        custom     = custom_mod,
        mount_args = mount_args,
        image      = race_spec['image'],
        command    = quoted_string
    )

def singularity_execution_cmdline(state,log,metadata, race_spec, dirs):
    #for running in subprocess
    quoted_string = ' '.join(map(pipes.quote,race_spec['argv']))
    honor_mounts = [x for x in race_spec['mounts'] if x['destination'] == '/recast_auth'] + [{'source': dirs['datamount'], 'destination': dirs['datamount'], 'type': 'bind'}] 
 
    mount_args = ''
    for s in honor_mounts:
        mount_args += ' -B {source}:{destination}'.format(
            source = s['source'],
            destination = s['destination']
        )

    
    return 'singularity exec -C {mount_args} --pwd {work} -H {home} docker://{image} {command}'.format(
        mount_args  = mount_args,
        work = dirs['work'],
        home = dirs['home'],
        image	   = race_spec['image'],
        command    = quoted_string
    )

def script_argv(environment,job,log):
    script = job['script']
    interpreter = job['interpreter']

    log.debug('script is:')
    log.debug('\n--------------\n'+script+'\n--------------')

    indocker = interpreter
    envmod = 'source {} && '.format(environment['envscript']) if environment['envscript'] else ''
    in_docker_cmd = envmod+indocker
    return ['sh', '-c', in_docker_cmd], script

def command_argv(environment,job,log):
    log.debug('''\n\
--------------
running one liner in container.
command: {command}
--------------
    '''.format(command = job['command']))

    envmod = 'source {} && '.format(environment['envscript']) if environment['envscript'] else ''
    in_docker_cmd = envmod + job['command']
    return ['sh', '-c', in_docker_cmd], None

def execute_and_tail_subprocess(config,metadata,state,log,command_string,stdin_content = None, logging_topic = 'execution'):
    log.debug('command: \n%s',command_string)
    log.debug('stdin if any: %s', stdin_content)
    if config.dry_run():
        return
    try:
        with logutils.setup_logging_topic(config,metadata,state,logging_topic, return_logger = True) as subproclog:

            proc = None
            if stdin_content:
                log.debug('stdin: \n%s',stdin_content)
                argv = shlex.split(command_string)
                log.debug('argv: %s', argv)
                proc = subprocess.Popen(argv,
                                        stdin = subprocess.PIPE,
                                        stderr = subprocess.STDOUT,
                                        stdout = subprocess.PIPE,
                                        bufsize=1,
                                        close_fds = True)
                proc.stdin.write(stdin_content.encode('utf-8'))
                proc.stdin.close()
            else:
                proc = subprocess.Popen(shlex.split(command_string), stderr = subprocess.STDOUT, stdout = subprocess.PIPE, bufsize=1, close_fds = True)

            log.debug('started subprocess with pid %s. now wait to finish',proc.pid)
            time.sleep(0.5)

            try: #some issues on some linux machines.. swallow exception
                log.debug('process children: %s',[x for x in psutil.Process(proc.pid).children(recursive = True)])
            except:
                pass

            for line in iter(proc.stdout.readline, b''):
                subproclog.info(line.strip())
            while proc.poll() is None:
                pass
            proc.stdout.close()
        log.debug('container execution subprocess finished. return code: %s',proc.returncode)
        if proc.returncode:
            log.error('non-zero return code raising exception')
            raise subprocess.CalledProcessError(returncode =  proc.returncode, cmd = command_string)
        log.debug('moving on from subprocess execution: {}'.format(command_string))
    except subprocess.CalledProcessError as exc:
        log.exception('subprocess failed. code: %s,  command %s',exc.returncode,exc.cmd)
        raise RuntimeError('failed container execution subprocess. %s',command_string)
    except:
        log.exception("Unexpected error: %s",sys.exc_info())
        raise
    finally:
        log.debug('finally for %s',command_string)

def race_spec(config,state,environment,log,job):
    if 'command' in job:
        container_argv, container_stdin = command_argv(environment,job,log)
    elif 'script' in job:
        container_argv, container_stdin = script_argv(environment,job,log)
    else:
        raise RuntimeError('do not know yet how to run this...')

    # volume mounts (resources, parameter mounts and state mounts)
    state_mounts = state_context_to_mounts(config,state)
    par_mounts   = prepare_par_mounts(config,environment['par_mounts'], state)
    rsrcs_opts, rsrcs_mounts = resource_mounts(config,state,environment,log)

    return {
        'mounts'  : state_mounts + par_mounts + rsrcs_mounts,
        'image'   : ':'.join([environment['image'],environment['imagetag']]),
        'workdir' : environment['workdir'],
        'argv'    : container_argv,
        'stdin'   : container_stdin
    }

def run_containers_in_docker_runtime(config,state,log,metadata,race_spec):
    if config.container_config.pull_software():
        execute_and_tail_subprocess(config,metadata,state,log,'docker pull {}'.format(race_spec['image']), logging_topic = 'pull')

    cmdline = docker_execution_cmdline(config,state,log,metadata,race_spec)
    execute_and_tail_subprocess(config,metadata,state,log,cmdline, stdin_content = race_spec['stdin'], logging_topic = 'run')

def run_containers_in_singularity_runtime(config,state,log,metadata,race_spec):
    import tempfile
    import shutil

    tmpdir_home = tempfile.mkdtemp(prefix = '_sing_home_')
    tmpdir_work = tempfile.mkdtemp(prefix = '{}/'.format(tmpdir_home))
    homemount = '/'.join(os.path.expanduser('~').split('/')[:2])

    cmdline = singularity_execution_cmdline(state,log,metadata,race_spec, dirs = {
        'work': tmpdir_work, 'home': tmpdir_home, 'datamount': homemount
    })
    execute_and_tail_subprocess(config,metadata,state,log,cmdline, stdin_content = race_spec['stdin'], logging_topic = 'run')
    shutil.rmtree(tmpdir_home)


@executor('docker-encapsulated')
def docker_enc_handler(config,environment,state,job,metadata):

    with logutils.setup_logging_topic(config,metadata,state,'step',return_logger = True) as log:
        rspec = race_spec(config,state,environment,log,job)

        log.debug('rspec is\n{}'.format(json.dumps(rspec, indent = 4)))

        runtimes = {
            'docker'     : run_containers_in_docker_runtime,
            'singularity': run_containers_in_singularity_runtime
        }
        run = runtimes[config.container_config.container_runtime()]
        run(config,state,log,metadata,rspec)

@executor('noop-env')
def noop_env(config,environment,state,job,metadata):
    with logutils.setup_logging_topic(config,metadata,state,'step',return_logger = True) as log:
        log.info('state is: %s',state)
        log.info('would be running this job: %s',job)

@executor('localproc-env')
def localproc_env(config,environment,state,job,metadata):
    with logutils.setup_logging_topic(config,metadata,state,'step',return_logger = True) as log:
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
def manual_env(config,environment,state,job,metadata):
    instructions = environment['instructions']
    ctx = yaml.safe_dump(state,default_flow_style = False)
    click.secho(instructions, fg = 'blue')
    click.secho(ctx, fg = 'cyan')

@executor('test-env')
def test_process(config,environment,state,job,metadata):
    with logutils.setup_logging_topic(config,metadata,state,'step',return_logger = True) as log:
        log.info('a complicated test environment')
        log.info('job:  {}'.format(job))
        log.info('env:  {}'.format(environment))
        log.info('state {}'.format(state))
