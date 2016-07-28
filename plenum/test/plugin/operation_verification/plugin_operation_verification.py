from plenum.common.types import PLUGIN_TYPE_VERIFICATION


class StatefulVerificationPlugin:
    pluginType = PLUGIN_TYPE_VERIFICATION

    validOpTypes = ['buy', 'sell']

    def __init__(self):
        self.count = 0

    def verify(self, operation):

        assert operation['type'] in self.validOpTypes, \
            'type must be one of {}'.format(self.validOpTypes)
        try:
            amt = int(operation['amount'])
            assert amt >= 0, 'amount must be >= 0'
        except ValueError as exc:
            raise RuntimeError('invalid amount') from exc

        self.count += 1
