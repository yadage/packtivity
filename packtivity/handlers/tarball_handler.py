import logging
import os
import subprocess
import sys

import packtivity.utils as utils
from packtivity.handlers.environment_handlers import docker_run_cmd, prepare_full_docker_with_oneliner, run_docker_with_script, remove_docker_image


def tarball_handler(environment, context, job):
    url = environment['url']
    image = environment['image']
    nametag = context.nametag

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
    metadir = '{}/_packtivity'.format(context.readwrite[0])
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
        run_docker_with_script(context, environment, job, log)
    else:
        remove_docker_image(image=image, log_filename=os.path.join(metadir, '%s.docker.rmi.log' % nametag), logger=log)
        raise RuntimeError('do not know yet how to run this...')
    remove_docker_image(image=image, log_filename=os.path.join(metadir, '%s.docker.rmi.log' % nametag), logger=log)