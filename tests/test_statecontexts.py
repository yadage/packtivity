
import packtivity.statecontexts.posixfs_context as state

def test_mkcontext():
	context = state.make_new_context('aname')
	assert context['readonly'] == []
	assert type(context['readwrite']) == list
	assert len(context['readwrite']) == 1

def test_mkcontext_in_old(tmpdir):
	oldcontext = {'readonly':[], 'readwrite': [str(tmpdir)]}
	context = state.make_new_context('aname', oldcontext = oldcontext)
	assert context['readonly'] == [str(tmpdir)]
	assert type(context['readwrite']) == list
	assert len(context['readwrite']) == 1
	assert str(tmpdir) in context['readonly'][0] # is nested

def test_mkcontext_in_old_create(tmpdir):
	oldcontext = {'readonly':[], 'readwrite': [str(tmpdir)]}
	context = state.make_new_context('aname', oldcontext = oldcontext, create = True)
	assert context['readonly'] == [str(tmpdir)]
	assert type(context['readwrite']) == list
	assert len(context['readwrite']) == 1
	assert str(tmpdir) in context['readonly'][0] # is nested
	assert tmpdir.join('aname').check()

