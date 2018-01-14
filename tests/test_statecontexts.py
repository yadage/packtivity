import os
from packtivity.typedleafs import TypedLeafs
def test_pack_call_local(tmpdir,basic_localfs_state):

    pars = TypedLeafs({'parcard': ['{workdir}/parcard.dat'], 'banner_file': '{workdir}/banner.txt'})

    newpars = basic_localfs_state.model(pars)

    assert type(pars) == TypedLeafs
    assert newpars['banner_file'] == os.path.join(str(tmpdir), 'banner.txt')
    assert newpars['parcard'][0] == os.path.join(str(tmpdir), 'parcard.dat')
