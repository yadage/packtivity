import importlib
import os

def enable_plugins():
	plugin_module = os.environ.get('PACKTIVITY_PLUGINS',None)
	if plugin_module:
		importlib.import_module(plugin_module)