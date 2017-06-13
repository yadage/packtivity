def test_pack_call(tmpdir,localproc_pack):
	context = {'readonly': [], 'readwrite': [str(tmpdir)]}
	pars =  {'outputfile':str(tmpdir.join('helloworld.txt'))}
	localproc_pack(parameters = pars, context = context)
	assert tmpdir.join('helloworld.txt').check()

def test_pack_call(tmpdir,dockeproc_pack):
	context = {'readonly': [], 'readwrite': [str(tmpdir)]}
	pars =  {'outputfile':str(tmpdir.join('helloworld.txt'))}
	dockeproc_pack(parameters = pars, context = context)
	assert tmpdir.join('helloworld.txt').check()

def test_pack_call(tmpdir,dockeproc_script_pack):
	context = {'readonly': [], 'readwrite': [str(tmpdir)]}
	pars =  {'outputfile':str(tmpdir.join('helloworld.txt'))}
	dockeproc_script_pack(parameters = pars, context = context)
	assert tmpdir.join('helloworld.txt').check()

def test_pack_call_async(tmpdir,dockeproc_script_pack,default_async):
	context = {'readonly': [], 'readwrite': [str(tmpdir)]}
	pars =  {'outputfile':str(tmpdir.join('helloworld.txt'))}
	dockeproc_script_pack(parameters = pars, context = context, asyncbackend = default_async, asyncwait = True)
	assert tmpdir.join('helloworld.txt').check()

def test_pack_call_async(tmpdir,dockeproc_script_pack,default_async):
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

