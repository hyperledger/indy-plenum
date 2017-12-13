from plenum.common.types import PLUGIN_TYPE_VERIFICATION


class NameAndAgeVerifier:
    pluginType = PLUGIN_TYPE_VERIFICATION
    supportsCli = True

    @staticmethod
    def verify(operation):
        assert len(operation['name']) <= 50, 'name too long'
        try:
            age = int(operation['age'])
            assert age >= 0, 'age must be >= 0'
        except ValueError as exc:
            raise RuntimeError('invalid age') from exc
