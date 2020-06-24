from packtivity.asyncbackends import MultiProcBackend
from packtivity import datamodel as pdm


def test_create_multiproc():
    MultiProcBackend(4)


def test_multiproc(tmpdir, basic_localfs_state, localproc_packspec):
    basic_localfs_state.ensure()
    pars = pdm.create({"outputfile": "{workdir}/helloworld.txt"})
    backend = MultiProcBackend(2)
    proxy = backend.submit(localproc_packspec, pars, basic_localfs_state)
    while not backend.ready(proxy):
        pass
    assert backend.successful(proxy)
    assert backend.result(proxy).json() == {
        "output": str(tmpdir.join("helloworld.txt"))
    }
    assert tmpdir.join("helloworld.txt").check() == True


def test_multiproc_fail(tmpdir, basic_localfs_state, localproc_pack_fail):
    basic_localfs_state.ensure()
    pars = pdm.create({"outputfile": "{workdir}/helloworld.txt"})

    backend = MultiProcBackend(2)
    proxy = backend.submit(localproc_pack_fail, pars, basic_localfs_state)
    while not backend.ready(proxy):
        pass
    assert backend.successful(proxy) == False
    backend.fail_info(proxy)
