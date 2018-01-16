from packtivity.handlers.publisher_handlers import handlers
from packtivity.typedleafs import TypedLeafs

from packtivity.handlers.execution_handlers import run_docker_with_oneliner, docker_execution_cmdline
import logging
import monkeypatch

def test_docker_cvmfs(tmpdir,basic_localfs_state, docker_env_resources, monkeypatch):
	state = basic_localfs_state
	environment = docker_env_resources.spec['environment']
	log = logging.getLogger('test')
	job = {'command': 'echo hello world'}
	container_argv, stdin = run_docker_with_oneliner(environment,job,log)

	assert container_argv == ['sh','-c','echo hello world']
	assert stdin == None

	cmdline = docker_execution_cmdline(
		state,environment,log,{'name':'myname'},
		stdin = stdin,
		cmd_argv = container_argv
	)
	assert '-v /cvmfs:/cvmfs' in cmdline

	monkeypatch.setenv('PACKTIVITY_CVMFS_LOCATION','/here/cvmfs')
	cmdline = docker_execution_cmdline(
		state,environment,log,{'name':'myname'},
		stdin = stdin,
		cmd_argv = container_argv
	)
	assert '-v /here/cvmfs:/cvmfs' in cmdline

	monkeypatch.setenv('PACKTIVITY_CVMFS_SOURCE','voldriver')
	cmdline = docker_execution_cmdline(
		state,environment,log,{'name':'myname'},
		stdin = stdin,
		cmd_argv = container_argv
	)

	assert '-v atlas-condb.cern.ch:/cvmfs/atlas-condb.cern.ch:rw' in cmdline
	assert '-v sft.cern.ch:/cvmfs/sft.cern.ch:rw' in cmdline
	assert '-v atlas.cern.ch:/cvmfs/atlas.cern.ch:rw' in cmdline
	assert '--security-opt label:disable' in cmdline


def test_docker_auth(tmpdir,basic_localfs_state, docker_env_resources, monkeypatch):
	state = basic_localfs_state
	environment = docker_env_resources.spec['environment']
	log = logging.getLogger('test')
	job = {'command': 'echo hello world'}
	container_argv, stdin = run_docker_with_oneliner(environment,job,log)

	assert container_argv == ['sh','-c','echo hello world']
	assert stdin == None

	cmdline = docker_execution_cmdline(
		state,environment,log,{'name':'myname'},
		stdin = stdin,
		cmd_argv = container_argv
	)
	assert '-v /home/recast/recast_auth:/recast_auth:rw' in cmdline

	monkeypatch.setenv('PACKTIVITY_AUTH_LOCATION','/here')
	cmdline = docker_execution_cmdline(
		state,environment,log,{'name':'myname'},
		stdin = stdin,
		cmd_argv = container_argv
	)
	assert '-v /here:/recast_auth:rw' in cmdline
