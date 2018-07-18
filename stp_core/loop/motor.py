from stp_core.common.log import getlogger
from stp_core.loop.looper import Prodable
from stp_core.loop.startable import Status

logger = getlogger()

# TODO: move it to plenum-util repo


class Motor(Prodable):
    """
    Base class for Prodable that includes status management.
    Subclasses are responsible for changing status from starting to started.
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
            logger.info("{} changing status from {} to {}".format(self, old.name, value.name))
            self._statusChanged(old, value)

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

    def start(self, loop):
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
            logger.debug("{} is already {}".format(self, self.status.name))
        else:
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

    async def prod(self, limit) -> int:
        raise NotImplementedError("{} must implement this method".format(self))
