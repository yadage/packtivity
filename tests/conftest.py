import pytest
import packtivity
import packtivity.utils
import packtivity.syncbackends
import packtivity.asyncbackends
from packtivity.statecontexts.posixfs_context import LocalFSState


@pytest.fixture()
def localproc_pack(tmpdir):
    return packtivity.pack_object.fromspec("tests/testspecs/localtouchfile.yml")


@pytest.fixture()
def basic_localfs_state(tmpdir):
    return LocalFSState([str(tmpdir)])


@pytest.fixture
def default_handler_config():
    return packtivity.syncbackends.packconfig()


@pytest.fixture()
def localproc_pack_fail(tmpdir):
    return packtivity.pack_object.fromspec("tests/testspecs/localtouchfail.yml")


@pytest.fixture()
def docker_pack_fail(tmpdir):
    return packtivity.pack_object.fromspec("tests/testspecs/dockerfail.yml")


@pytest.fixture()
def docker_script_pack_fail(tmpdir):
    return packtivity.pack_object.fromspec("tests/testspecs/dockerfail_script.yml")


@pytest.fixture()
def localproc_packspec(tmpdir):
    return packtivity.utils.load_packtivity("tests/testspecs/localtouchfile.yml")


@pytest.fixture()
def dockeproc_pack(tmpdir):
    return packtivity.pack_object.fromspec("tests/testspecs/dockertouchfile.yml")


@pytest.fixture()
def dockeproc_script_pack(tmpdir):
    return packtivity.pack_object.fromspec("tests/testspecs/dockertouchfile_script.yml")


@pytest.fixture()
def docker_touchfile_workdir(tmpdir):
    return packtivity.pack_object.fromspec(
        "tests/testspecs/environment_tests/touchfile_docker_inworkdir.yml"
    )


@pytest.fixture()
def docker_env_resources(tmpdir):
    return packtivity.pack_object.fromspec(
        "tests/testspecs/environment_tests/resources_docker.yml"
    )


@pytest.fixture()
def docker_env_parmounts(tmpdir):
    return packtivity.pack_object.fromspec(
        "tests/testspecs/environment_tests/resources_parmounts.yml"
    )


@pytest.fixture()
def fromjq_pub_default(tmpdir):
    return packtivity.pack_object.fromspec(
        "tests/testspecs/publisher_tests/fromjq-pub-default.yml"
    )


@pytest.fixture()
def default_sync():
    return packtivity.syncbackends.defaultsyncbackend()


@pytest.fixture()
def default_async():
    return packtivity.asyncbackends.MultiProcBackend(2)
