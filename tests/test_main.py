import os
import pytest
import subprocess

def test_pack_call_local(tmpdir,localproc_pack):
	context = {'readonly': [], 'readwrite': [str(tmpdir)]}
	pars =  {'outputfile':str(tmpdir.join('helloworld.txt'))}
	localproc_pack(parameters = pars, context = context)
	assert tmpdir.join('helloworld.txt').check()

def test_pack_call_docker(tmpdir,dockeproc_pack):
	context = {'readonly': [], 'readwrite': [str(tmpdir)]}
	pars =  {'outputfile':str(tmpdir.join('helloworld.txt'))}
	dockeproc_pack(parameters = pars, context = context)
	assert tmpdir.join('helloworld.txt').check()

def test_pack_call_local_fail(tmpdir,localproc_pack_fail,default_async):
	context = {'readonly': [], 'readwrite': [str(tmpdir)]}
	pars =  {'outputfile':str(tmpdir.join('helloworld.txt'))}
	with pytest.raises(subprocess.CalledProcessError):
		localproc_pack_fail(parameters = pars, context = context,)
	assert tmpdir.join('helloworld.txt').check()

def test_pack_call_docker_fail(tmpdir,docker_pack_fail,default_async):
	context = {'readonly': [], 'readwrite': [str(tmpdir)]}
	pars =  {'outputfile':str(tmpdir.join('helloworld.txt'))}
	with pytest.raises(RuntimeError):
		docker_pack_fail(parameters = pars, context = context,)

def test_pack_call_docker_script_fail(tmpdir,docker_script_pack_fail,default_async):
	context = {'readonly': [], 'readwrite': [str(tmpdir)]}
	pars =  {'outputfile':str(tmpdir.join('helloworld.txt'))}
	with pytest.raises(RuntimeError):
		docker_script_pack_fail(parameters = pars, context = context,)

def test_pack_call_docker_script(tmpdir,dockeproc_script_pack):
	context = {'readonly': [], 'readwrite': [str(tmpdir)]}
	pars =  {'outputfile':str(tmpdir.join('helloworld.txt'))}
	dockeproc_script_pack(parameters = pars, context = context)
	assert tmpdir.join('helloworld.txt').check()

def test_pack_call_docker_async(tmpdir,dockeproc_script_pack,default_async):
	context = {'readonly': [], 'readwrite': [str(tmpdir)]}
	pars =  {'outputfile':str(tmpdir.join('helloworld.txt'))}
	dockeproc_script_pack(parameters = pars, context = context, asyncbackend = default_async, asyncwait = True)
	assert tmpdir.join('helloworld.txt').check()

def test_pack_call_docker_script_async(tmpdir,dockeproc_script_pack,default_async):
	context = {'readonly': [], 'readwrite': [str(tmpdir)]}
	pars =  {'outputfile':str(tmpdir.join('helloworld.txt'))}
	proxy = dockeproc_script_pack(parameters = pars, context = context, asyncbackend = default_async)
	while not default_async.ready(proxy): pass
	default_async.result(proxy)
	assert tmpdir.join('helloworld.txt').check()

def test_pack_prepublish(tmpdir,localproc_pack,default_sync):
	context = {'readonly': [], 'readwrite': [str(tmpdir)]}
	pars =  {'outputfile':str(tmpdir.join('helloworld.txt'))}

	assert default_sync.prepublish(localproc_pack.spec,pars,context) == {
		'output': str(tmpdir.join('helloworld.txt'))
	}
