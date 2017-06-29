from packtivity.asyncbackends import MultiProcBackend

def test_create_multiproc():
	MultiProcBackend(4)

def test_multiproc(tmpdir,basic_localfs_state,localproc_packspec):
	pars =  {'outputfile': basic_localfs_state.contextualize_data('{workdir}/helloworld.txt')}
	backend = MultiProcBackend(2)
	proxy = backend.submit(localproc_packspec,pars,basic_localfs_state)
	while not backend.ready(proxy):
		pass
	assert backend.successful(proxy)
	assert backend.result(proxy) == {'output': str(tmpdir.join('helloworld.txt'))}
	assert tmpdir.join('helloworld.txt').check() == True


def test_multiproc_fail(tmpdir,basic_localfs_state,localproc_pack_fail):
	pars =  {'outputfile': basic_localfs_state.contextualize_data('{workdir}/helloworld.txt')}

	backend = MultiProcBackend(2)
	proxy = backend.submit(localproc_pack_fail,pars,basic_localfs_state)
	while not backend.ready(proxy):
		pass
	assert backend.successful(proxy) == False
	backend.fail_info(proxy)

