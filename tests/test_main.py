import os
import pytest
import subprocess

def test_pack_call_local(tmpdir,basic_localfs_state,localproc_pack):
	basic_localfs_state.ensure()
	pars =  {'outputfile': basic_localfs_state.contextualize_data('{workdir}/helloworld.txt')}
	localproc_pack(parameters = pars, state = basic_localfs_state)
	assert tmpdir.join('helloworld.txt').check()

def test_pack_call_docker(tmpdir,basic_localfs_state,dockeproc_pack):
	basic_localfs_state.ensure()
	pars =  {'outputfile': basic_localfs_state.contextualize_data('{workdir}/helloworld.txt')}
	dockeproc_pack(parameters = pars, state = basic_localfs_state)
	assert tmpdir.join('helloworld.txt').check()

def test_pack_call_local_fail(tmpdir,basic_localfs_state,localproc_pack_fail,default_async):
	basic_localfs_state.ensure()
	pars =  {'outputfile': basic_localfs_state.contextualize_data('{workdir}/helloworld.txt')}
	with pytest.raises(subprocess.CalledProcessError):
		localproc_pack_fail(parameters = pars, state = basic_localfs_state)
	assert tmpdir.join('helloworld.txt').check()

def test_pack_call_docker_fail(tmpdir,basic_localfs_state,docker_pack_fail,default_async):
	basic_localfs_state.ensure()
	pars =  {'outputfile': basic_localfs_state.contextualize_data('{workdir}/helloworld.txt')}
	with pytest.raises(RuntimeError):
		docker_pack_fail(parameters = pars, state = basic_localfs_state)

def test_pack_call_docker_script_fail(tmpdir,basic_localfs_state,docker_script_pack_fail,default_async):
	basic_localfs_state.ensure()
	pars =  {'outputfile': basic_localfs_state.contextualize_data('{workdir}/helloworld.txt')}
	with pytest.raises(RuntimeError):
		docker_script_pack_fail(parameters = pars, state = basic_localfs_state)

def test_pack_call_docker_script(tmpdir,basic_localfs_state,dockeproc_script_pack):
	basic_localfs_state.ensure()
	pars =  {'outputfile': basic_localfs_state.contextualize_data('{workdir}/helloworld.txt')}
	dockeproc_script_pack(parameters = pars, state = basic_localfs_state)
	assert tmpdir.join('helloworld.txt').check()

def test_pack_call_docker_async(tmpdir,basic_localfs_state,dockeproc_script_pack,default_async):
	basic_localfs_state.ensure()
	pars =  {'outputfile': basic_localfs_state.contextualize_data('{workdir}/helloworld.txt')}
	dockeproc_script_pack(parameters = pars, state = basic_localfs_state, asyncbackend = default_async, asyncwait = True)
	assert tmpdir.join('helloworld.txt').check()

def test_pack_call_docker_script_async(tmpdir,basic_localfs_state,dockeproc_script_pack,default_async):
	basic_localfs_state.ensure()
	pars =  {'outputfile': basic_localfs_state.contextualize_data('{workdir}/helloworld.txt')}
	proxy = dockeproc_script_pack(parameters = pars, state = basic_localfs_state, asyncbackend = default_async)
	while not default_async.ready(proxy): pass
	default_async.result(proxy)
	assert tmpdir.join('helloworld.txt').check()

def test_pack_prepublish(tmpdir,basic_localfs_state,localproc_pack,default_sync):
	basic_localfs_state.ensure()
	pars =  {'outputfile': basic_localfs_state.contextualize_data('{workdir}/helloworld.txt')}

	assert default_sync.prepublish(localproc_pack.spec,pars,basic_localfs_state) == {
		'output': str(tmpdir.join('helloworld.txt'))
	}
