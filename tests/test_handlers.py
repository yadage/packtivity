import pytest
import packtivity.syncbackends

@pytest.fixture
def default_handler_config():
    return packtivity.syncbackends.packconfig()

def test_build_oneline_job(default_handler_config):
    job = packtivity.syncbackends.build_job(
    {
        'process_type':'string-interpolated-cmd',
        'cmd': 'hello {one} {two}'},
        {'one':'ONE', 'two':'TWO'},
        default_handler_config
    )
    assert 'command' in job
    assert job['command'] == 'hello ONE TWO'

def test_build_script_job(default_handler_config):
    job = packtivity.syncbackends.build_job(
    {
        'process_type':'interpolated-script-cmd',
        'interpreter':'sh',
        'script': 'hello {one} {two}\n echo another line {two}'},
        {'one':'ONE', 'two':'TWO'},
        default_handler_config
    )
    assert 'script' in job
    assert 'interpreter' in job
