import getpass
import os
import shutil
import sys


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
