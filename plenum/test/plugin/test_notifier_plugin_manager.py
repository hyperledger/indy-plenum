import pip.utils as utils
from plenum.test.helper import randomText, mockGetInstalledDistributions, \
    mockImportModule
from functools import partial
import importlib


def testPluginManagerFindsPlugins(monkeypatch, pluginManager):
    validPackagesCnt = 5
    invalidPackagesCnt = 10
    validPackages = [pluginManager.prefix + randomText(10)
                     for _ in range(validPackagesCnt)]
    invalidPackages = [randomText(10) for _ in range(invalidPackagesCnt)]

    monkeypatch.setattr(utils, 'get_installed_distributions',
                        partial(mockGetInstalledDistributions, packages=validPackages+invalidPackages))
    assert len(pluginManager._findPlugins()) == validPackagesCnt


def testPluginManagerImportsPlugins(monkeypatch, pluginManager):
    packagesCnt = 3
    packages = [pluginManager.prefix + randomText(10)
                     for _ in range(packagesCnt)]

    monkeypatch.setattr(utils, 'get_installed_distributions',
                        partial(mockGetInstalledDistributions,
                                packages=packages))
    monkeypatch.setattr(importlib, 'import_module', mockImportModule)

    importedPlugins, foundPlugins = pluginManager.importPlugins()
    assert importedPlugins == foundPlugins and importedPlugins == packagesCnt


def testPluginManagerSendsMessage(pluginManagerWithImportedModules):
    topic = randomText(10)
    message = randomText(20)
    sent, pluginCnt = pluginManagerWithImportedModules\
        ._sendMessage(topic, message)
    assert sent == 3

