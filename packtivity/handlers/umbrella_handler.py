import json
import os
import subprocess
import tempfile
from tempfile import mkstemp
from urllib import urlretrieve, ContentTooShortError
from urllib2 import urlopen

import packtivity.utils as utils


def umbrella(environment, context, job):
    """
    Handler for running Packtivity with an Umbrella backend.
    
    options will contain the full umbrella command as a list
    
    :param environment: contains a dictionary of the Umbrella specification file
    :param context: 
    :param job: dictionary containing the command that is supposed to be run
    :return: 
    """
    print job

    metadir = '{}/_packtivity'.format(context.readwrite[0])
    context.metadir = metadir

    if not os.path.exists(metadir):
        utils.mkdir_p(metadir)
    fp = open(os.path.join(metadir, "umbrella_output.txt"), "w")

    # Check if the spec_url is actually a url or just a file path
    spec_file = None
    spec_path = None
    specification_file = ""
    temp_json_spec_fd, temp_json_spec_file_path = tempfile.mkstemp()
    json_spec = open(temp_json_spec_file_path, 'w+')

    # If a JSON specification file is included in the packtivity spec
    # JSON specification can be either path to local file or URL
    # MUST BE JSON!!
    if 'spec_url' in environment:
        try:
            f = urlopen(environment['spec_url'])  # tries to open the url
            spec_fd, temp_spec_path = mkstemp()
            spec_file = spec_fd
            spec_path = temp_spec_path
            # urlretrieve will throw UrlError, HTTPError, or ContentTooShortError
            (filename, headers) = urlretrieve(environment['spec_url'], temp_spec_path)
            specification_file = filename
        except ContentTooShortError:
            print("URL Content is Too Short! ")
        except ValueError:  # invalid URL
            specification_file = environment['spec_url']

    # This block within the if 'spec' is to handle a JSON reference to a YAML file in the packtivity spec
    # MUST BE YAML!!
    if 'spec' in environment:
        json.dump(environment['spec'], json_spec, indent=2)
        specification_file = os.path.abspath(json_spec.name)
        json_spec.close()

    # what spec is the umbrella command using?
    print("Using specification file: ", specification_file)

    command = job.get('command', None)
    tempdir = tempfile.mkdtemp()
    logfile = job.get('logfile', 'umbrella.log')
    # docker_mod = prepare_docker(context=context, do_cvmfs=False, do_auth=False, log=logfile)

    readwrites  = context.readwrite
    readonlies = context.readonly
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
    print 'OPTIONS'
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

            if spec_file and spec_path:
                os.close(spec_file)
                os.remove(spec_path)

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