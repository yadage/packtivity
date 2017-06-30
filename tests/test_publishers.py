from packtivity.handlers.publisher_handlers import handlers

def test_parpub(tmpdir,basic_localfs_state):
	pub = {
		'publisher_type': 'frompar-pub',
		'outputmap': {
			'hello': 'mypar'
		}
	}
	pars = {
		'mypar': 'myvalue'
	}
	pubbed = handlers['frompar-pub']['default'](pub,pars,basic_localfs_state)
	assert pubbed == {'hello':'myvalue'}


def test_interp_pub(tmpdir,basic_localfs_state):
	pub = {
		'publisher_type': 'interpolated-pub',
		'publish': {
			'hello': 'hello_{mypar}_world',
		},
		'glob': False
	}
	pars = {
		'mypar': 'myvalue'
	}
	pubbed = handlers['interpolated-pub']['default'](pub,pars,basic_localfs_state)
	assert pubbed == {'hello': 'hello_myvalue_world'}

def test_interp_pub_glob(tmpdir,basic_localfs_state):
	tmpdir.join('hello_myvalue_1.txt').ensure(file = True)
	tmpdir.join('hello_myvalue_2.txt').ensure(file = True)
	pub = {
		'publisher_type': 'interpolated-pub',
		'publish': {
			'hello': '{workdir}/hello_{mypar}_*.txt',
		},
		'glob': True
	}
	pars = {
		'mypar': 'myvalue'
	}
	pubbed = handlers['interpolated-pub']['default'](pub,pars,basic_localfs_state)

	filelist = map(str,[tmpdir.join('hello_myvalue_1.txt'),tmpdir.join('hello_myvalue_2.txt')])
	assert set(pubbed['hello']) == set(filelist)


def test_glob_pub(tmpdir,basic_localfs_state):
	tmpdir.join('hello_1.txt').ensure(file = True)
	tmpdir.join('hello_2.txt').ensure(file = True)
	pub = {
		'publisher_type': 'fromglob-pub',
		'outputkey': 'hello',
		'globexpression': 'hello_*.txt',
	}
	pars = {
		'mypar': 'myvalue'
	}
	pubbed = handlers['fromglob-pub']['default'](pub,pars,basic_localfs_state)

	filelist = map(str,[tmpdir.join('hello_1.txt'),tmpdir.join('hello_2.txt')])
	assert set(pubbed['hello']) == set(filelist)

def test_yml_pub(tmpdir,basic_localfs_state):
	tmpdir.join('hello.yml').write('hello: world\n')
	pub = {
		'publisher_type': 'fromyaml-pub',
		'yamlfile': 'hello.yml',
	}
	pars = {
		'mypar': 'myvalue'
	}
	pubbed = handlers['fromyaml-pub']['default'](pub,pars,basic_localfs_state)
	assert pubbed == {'hello': 'world'}