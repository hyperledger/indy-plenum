import os


def getPluginPath(name):
    curPath = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(curPath, name)
