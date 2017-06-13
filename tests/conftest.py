import pytest
import packtivity
import packtivity.syncbackends
import packtivity.asyncbackends

@pytest.fixture()
def localproc_pack(tmpdir):
	return packtivity.pack_object.fromspec('tests/testspecs/localtouchfile.yml')

@pytest.fixture()
def localproc_pack_fail(tmpdir):
	return packtivity.pack_object.fromspec('tests/testspecs/localtouchfail.yml')

@pytest.fixture()
def localproc_packspec(tmpdir):
	return packtivity.load_pack('tests/testspecs/localtouchfile.yml')

@pytest.fixture()
def dockeproc_pack(tmpdir):
	pack = packtivity.pack_object.fromspec('tests/testspecs/dockertouchfile.yml')
	return pack

@pytest.fixture()
def dockeproc_script_pack(tmpdir):
	pack = packtivity.pack_object.fromspec('tests/testspecs/dockertouchfile_script.yml')
	return pack

@pytest.fixture()
def default_sync():
	return packtivity.syncbackends.defaultsyncbackend()

@pytest.fixture()
def default_async():
	return packtivity.asyncbackends.MultiProcBackend(2)