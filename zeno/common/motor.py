import logging

from zeno.common.looper import Prodable
from zeno.common.startable import Status


class Motor(Prodable):
    """
    Helper functions for Status.
    """

    def __init__(self):
        """
        Motor is initialized with a status of Stopped.
        """
        self._status = Status.stopped

    def get_status(self) -> Status:
        """
        Return the current status
        """
        return self._status

    def set_status(self, value):
        """
        Set the status of the motor to the specified value if not already set.
        """
        if not self._status == value:
            old = self._status
            self._status = value
            logging.debug("{} changing status from {} to {}".
                          format(self, old.name, value.name))
            self._statusChanged(old.name, value.name)

    status = property(fget=get_status, fset=set_status)

    def isReady(self):
        """
        Is the status in Status.ready()?
        """
        return self.status in Status.ready()

    def isGoing(self):
        """
        Is the status in Status.going()?
        """
        return self.status in Status.going()

    def start(self):
        """
        Set the status to Status.starting
        """
        self.status = Status.starting

    def stop(self, *args, **kwargs):
        """
        Set the status to Status.stopping and also call `onStopping`
        with the provided args and kwargs.
        """
        if self.status in (Status.stopping, Status.stopped):
            logging.warning("{} is already {}".format(self, self.status.name))
        self.status = Status.stopping
        self.onStopping(*args, **kwargs)
        self.status = Status.stopped

    def _statusChanged(self, old, new):
        """
        Perform some actions based on whether this node is ready or not.

        :param old: the previous status
        :param new: the current status
        """
        raise NotImplementedError("{} must implement this method".format(self))

    def onStopping(self, *args, **kwargs):
        """
        A series of actions to be performed when stopping the motor.
        """
        raise NotImplementedError("{} must implement this method".format(self))

    def prod(self, limit) -> int:
        raise NotImplementedError("{} must implement this method".format(self))
