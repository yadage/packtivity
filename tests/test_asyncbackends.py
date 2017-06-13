from packtivity.asyncbackends import MultiProcBackend

def test_create_multiproc():
	MultiProcBackend(4)

def test_multiproc(tmpdir,localproc_packspec):
	context = {'readonly': [], 'readwrite': [str(tmpdir)]}
	pars =  {'outputfile':str(tmpdir.join('helloworld.txt'))}

	backend = MultiProcBackend(2)
	proxy = backend.submit(localproc_packspec,pars,context)
	while not backend.ready(proxy):
		pass
	assert backend.successful(proxy)
	assert backend.result(proxy) == {'output': str(tmpdir.join('helloworld.txt'))}
	assert tmpdir.join('helloworld.txt').check() == True


def test_multiproc_fail(tmpdir,localproc_pack_fail):
	context = {'readonly': [], 'readwrite': [str(tmpdir)]}
	pars =  {'outputfile':str(tmpdir.join('helloworld.txt'))}

	backend = MultiProcBackend(2)
	proxy = backend.submit(localproc_pack_fail,pars,context)
	while not backend.ready(proxy):
		pass
	assert backend.successful(proxy) == False
	backend.fail_info(proxy)

