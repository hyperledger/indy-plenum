# This file is for short term, it will go away
import time


class RaetCoroGen:
    def __init__(self, created, clbk, msgQ):
        self.clbk = clbk
        self.created = created
        self.msgQ = msgQ

    @property
    def age(self):
        """
        Returns the time elapsed since this stack was created
        """
        return time.perf_counter() - self.created

    async def __aiter__(self):
        return self

    async def __anext__(self):
        while True:
            try:
                self.clbk(self.age)
                l = len(self.msgQ)
            except Exception as ex:
                if isinstance(ex, OSError) and \
                        len(ex.args) > 0 and \
                        ex.args[0] == 22:
                    logger.error("Error servicing stack {}: {}. This could be "
                                 "due to binding to an internal network "
                                 "and trying to route to an external one.".
                                 format(self.name, ex), extra={'cli': 'WARNING'})
                else:
                    logger.error("Error servicing stack {}: {} {}".
                                 format(self.name, ex, ex.args),
                                 extra={'cli': 'WARNING'})

                l = 0
            return l
