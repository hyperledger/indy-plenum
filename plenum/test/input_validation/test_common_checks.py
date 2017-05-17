import pytest

from plenum.test.input_validation.messages import messages, messages_names_shortcut


# TODO: check error messages


@pytest.mark.parametrize('descriptor', argvalues=messages, ids=messages_names_shortcut)
def test_message_valid(descriptor):
    for m in descriptor.positive_test_cases_valid_message:
        assert descriptor.klass(**m), 'Correct msg passes: {}'.format(m)


@pytest.mark.parametrize('descriptor', argvalues=messages, ids=messages_names_shortcut)
def test_message_missed_optional_field_pass(descriptor):
    for m in descriptor.positive_test_cases_missed_optional_field:
        assert descriptor.klass(**m), 'Correct msg passes: {}'.format(m)


@pytest.mark.parametrize('descriptor', argvalues=messages, ids=messages_names_shortcut)
def test_message_invalid_value_fail(descriptor):
    for m in descriptor.negative_test_cases_invalid_value:
        with pytest.raises(TypeError) as exc_info:
            descriptor.klass(**m)
        assert exc_info.match(r'Field \'.*\' validation error: .*')
            

@pytest.mark.parametrize('descriptor', argvalues=messages, ids=messages_names_shortcut)
def test_message_missed_required_field_fail(descriptor):
    for m in descriptor.negative_test_cases_missed_required_field:
        with pytest.raises(TypeError) as exc_info:
            descriptor.klass(**m)
        assert exc_info.match(r'Missed fields: .*')


@pytest.mark.parametrize('descriptor', argvalues=messages, ids=messages_names_shortcut)
def test_message_extra_field_fail(descriptor):
    for m in descriptor.negative_test_cases_extra_field:
        with pytest.raises(TypeError) as exc_info:
            descriptor.klass(**m)
        assert exc_info.match(r'Unknown field: .*')


@pytest.mark.parametrize('descriptor', argvalues=messages, ids=messages_names_shortcut)
def test_message_wrong_type_fail(descriptor):
    for m in descriptor.negative_test_cases_wrong_type:
        with pytest.raises(TypeError) as exc_info:
            descriptor.klass(**m)
        assert exc_info.match(r'Field \'.*\' validation error: expected types .*')
