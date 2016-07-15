class NameAndAgeVerifier:
    pluginType = 'VERIFICATION'
    supportsCli = True

    @staticmethod
    def verify(operation):
        assert len(operation['name']) <= 50, 'name too long'
        try:
            age = int(operation['age'])
            assert age >= 0, 'age must be >= 0'
        except ValueError as exc:
            raise RuntimeError('invalid age') from exc

