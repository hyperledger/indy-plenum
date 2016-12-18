import pip.utils as utils
from plenum.test.helper import randomText


def testPluginManagerFindsPlugins(monkeypatch, pluginManager):
    validPackagesCnt = 5
    invalidPackagesCnt = 10
    validPackages = [pluginManager.prefix + randomText(10)
                     for _ in range(validPackagesCnt)]
    invalidPackages = [randomText(10) for _ in range(invalidPackagesCnt)]

    def mockGetInstalledDistributions():
        packages = validPackages + invalidPackages
        ret = []
        for pkg in packages:
            obj = type('', (), {})()
            obj.key = pkg
            ret.append(obj)
        return ret

    monkeypatch.setattr(utils, 'get_installed_distributions',
                        mockGetInstalledDistributions)
    assert len(pluginManager.findPlugins()) == validPackagesCnt


