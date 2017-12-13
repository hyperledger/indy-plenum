import getpass
import os
import shutil
import sys
import errno


def getLoggedInUser():
    if sys.platform == 'wind32':
        return getpass.getuser()
    else:
        if 'SUDO_USER' in os.environ:
            return os.environ['SUDO_USER']
        else:
            return getpass.getuser()


def changeOwnerAndGrpToLoggedInUser(directory, raiseEx=False):
    loggedInUser = getLoggedInUser()
    try:
        shutil.chown(directory, loggedInUser, loggedInUser)
    except Exception as e:
        if raiseEx:
            raise e
        else:
            pass


def copyall(src, dst):
    if os.path.exists(dst):
        shutil.rmtree(dst)
    try:
        shutil.copytree(src, dst)
    except OSError as ex:
        if ex.errno == errno.ENOTDIR:
            shutil.copy(src, dst)
        else:
            raise
