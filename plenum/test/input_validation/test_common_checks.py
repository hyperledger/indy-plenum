import pytest

from plenum.test.input_validation.messages import messages, messages_names_shortcut


# TODO: check error messages


@pytest.mark.skip('INDY-78. Roll away new validation logic')
# @pytest.mark.parametrize('descriptor', argvalues=messages, ids=messages_names_shortcut)
def test_message_valid(descriptor):
    for m in descriptor.positive_test_cases_valid_message:
        assert descriptor.klass(**m), 'Correct msg passes: {}'.format(m)


@pytest.mark.skip('INDY-78. Roll away new validation logic')
# @pytest.mark.parametrize('descriptor', argvalues=messages, ids=messages_names_shortcut)
def test_message_missed_optional_field_pass(descriptor):
    for m in descriptor.positive_test_cases_missed_optional_field:
        assert descriptor.klass(**m), 'Correct msg passes: {}'.format(m)


@pytest.mark.skip('INDY-78. Roll away new validation logic')
# @pytest.mark.parametrize('descriptor', argvalues=messages, ids=messages_names_shortcut)
def test_message_invalid_value_fail(descriptor):
    for m in descriptor.negative_test_cases_invalid_value:
        with pytest.raises(TypeError, message='did not raise {}'.format(m)) as exc_info:
            descriptor.klass(**m)
        assert exc_info.match(r'validation error: .*')
            

@pytest.mark.skip('INDY-78. Roll away new validation logic')
# @pytest.mark.parametrize('descriptor', argvalues=messages, ids=messages_names_shortcut)
def test_message_missed_required_field_fail(descriptor):
    for m in descriptor.negative_test_cases_missed_required_field:
        with pytest.raises(TypeError, message='did not raise {}'.format(m)) as exc_info:
            descriptor.klass(**m)
        assert exc_info.match(r'validation error: missed fields .*')


@pytest.mark.skip('INDY-78. Roll away new validation logic')
# @pytest.mark.parametrize('descriptor', argvalues=messages, ids=messages_names_shortcut)
def test_message_extra_field_fail(descriptor):
    for m in descriptor.negative_test_cases_extra_field:
        with pytest.raises(TypeError, message='did not raise {}'.format(m)) as exc_info:
            descriptor.klass(**m)
        assert exc_info.match(r'validation error: unknown field .*')


@pytest.mark.skip('INDY-78. Roll away new validation logic')
# @pytest.mark.parametrize('descriptor', argvalues=messages, ids=messages_names_shortcut)
def test_message_wrong_type_fail(descriptor):
    for m in descriptor.negative_test_cases_wrong_type:
        with pytest.raises(TypeError, message='did not raise {}'.format(m)) as exc_info:
            descriptor.klass(**m)
        assert exc_info.match(r'validation error: .*')
