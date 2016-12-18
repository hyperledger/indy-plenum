import plenum.server.notifier_plugin_manager as pluginManager
import pip.utils as utils
from plenum.test.helper import randomText


def testPluginManagerFindsPlugins(monkeypatch):
    assert prefix in pluginManager
    assert sendMessage in pluginManager
    assert findPlugins in pluginManager

    validPackagesCnt = 5
    invalidPackagesCnt = 10
    validPackages = [pluginManager.prefix + randomText(10)
                     for _ in range(validPackagesCnt)]
    invalidPackages = [randomText(10) for _ in range(invalidPackagesCnt)]

    def mockGetInstalledDistributions():
        packages = validPackages + invalidPackages
        return ['{}==0'.format(package) for package in packages]

    monkeypatch.setattr(utils, 'get_installed_distributions',
                        mockGetInstalledDistributions)
    assert len(pluginManager.findPlugins()) == validPackagesCnt


