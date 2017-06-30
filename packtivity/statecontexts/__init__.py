import posixfs_context

def load_state(jsondata):
	if jsondata['state_type'] == 'localfs':
		return posixfs_context.LocalFSState.fromJSON(jsondata)
	raise TypeError('unknown state type {}'.format(jsondata['state_type']))

def load_provider(jsondata):
	if jsondata == None:
		return None
	if jsondata['state_provider_type'] == 'localfs_provider':
		return posixfs_context.LocalFSProvider.fromJSON(jsondata)
