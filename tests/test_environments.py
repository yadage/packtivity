from packtivity.handlers.publisher_handlers import handlers
from packtivity.typedleafs import TypedLeafs

from packtivity.handlers.environment_handlers import handlers
from packtivity.syncbackends import finalize_inputs

import logging

def test_docker_parmounts(tmpdir,basic_localfs_state, docker_env_parmounts):
	state = basic_localfs_state
	environment = docker_env_parmounts.spec['environment']

	parameters, state = finalize_inputs(TypedLeafs({'outputfile': '{workdir}/hello.txt'}), state)
	env = handlers[environment['environment_type']]['default'](environment,parameters,state)
	assert env['par_mounts'][0]['mountcontent'] == '"{}"'.format(parameters['outputfile'])
