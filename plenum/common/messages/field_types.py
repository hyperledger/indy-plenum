class FieldBase:

    _base_types = ()

    def __init__(self, optional=False):
        self.optional = optional

    def validate(self, val):
        type_er = self.__type_check(val)
        if type_er:
            return type_er
        return self._check(val)

    def _check(self, val):
        raise NotImplementedError

    def __type_check(self, val):
        for t in self._base_types:
            if isinstance(val, t):
                return
        return "expected types '{}', got '{}'" \
               "".format(', '.join(map(lambda x: x.__name__, self._base_types)), type(val).__name__)


class NonEmptyStringField(FieldBase):
    _base_types = (str,)

    def _check(self, val):
        if not val:
            return 'empty string'


class NonNegativeNumberField(FieldBase):

    _base_types = (int,)

    def _check(self, val):
        if val < 0:
            return 'negative value'


class IterableField(FieldBase):

    _base_types = (list, tuple)

    def __init__(self, inner_field_type: FieldBase, optional=False):
        self.optional = optional
        self.inner_field_type = inner_field_type
        super().__init__(optional=optional)

    def _check(self, val):
        for v in val:
            check_er = self.inner_field_type._check(v)
            if check_er:
                return check_er
