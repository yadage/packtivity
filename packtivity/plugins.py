import importlib
import os


def enable_plugins(modules=None):
    plugin_modules = modules or []
    fromenv = os.environ.get("PACKTIVITY_PLUGINS", "")
    if fromenv:
        plugin_modules += fromenv.split(",")
    if plugin_modules:
        for plugin in plugin_modules:
            importlib.import_module(plugin)
