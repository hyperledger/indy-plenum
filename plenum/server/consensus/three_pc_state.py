from plenum.common.channel import RxChannel


class ThreePCState:
    def __init__(self):
        pass

    def on_update(self) -> RxChannel:
        """
        Channel with important update events

        :return: channel to subscribe
        """
        raise NotImplemented
