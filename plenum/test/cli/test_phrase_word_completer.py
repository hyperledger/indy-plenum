from prompt_toolkit.completion import CompleteEvent, Completion
from prompt_toolkit.document import Document

from plenum.cli.phrase_word_completer import PhraseWordCompleter


def test_next_phrase_word_is_suggested_for_typed_word_being_its_prefix():
    completer = PhraseWordCompleter('add genesis transaction')
    document = Document('add genesis tra')
    complete_event = CompleteEvent(text_inserted=True)

    completions = list(completer.get_completions(document, complete_event))

    assert [Completion('transaction', -3)] == completions


def test_none_is_suggested_for_typed_word_not_being_next_phrase_word_prefix():
    completer = PhraseWordCompleter('add genesis transaction')
    document = Document('add tr')
    complete_event = CompleteEvent(text_inserted=True)

    completions = list(completer.get_completions(document, complete_event))

    assert [] == completions


def test_next_phrase_word_is_suggested_for_space():
    completer = PhraseWordCompleter('add genesis transaction')
    document = Document('add ')
    complete_event = CompleteEvent(text_inserted=True)

    completions = list(completer.get_completions(document, complete_event))

    assert [Completion('genesis')] == completions


def test_next_phrase_word_is_suggested_for_space_typed_inside_input():
    completer = PhraseWordCompleter('add genesis transaction')
    document = Document('add  transaction', 4)
    complete_event = CompleteEvent(text_inserted=True)

    completions = list(completer.get_completions(document, complete_event))

    assert [Completion('genesis')] == completions


def test_next_phrase_word_is_suggested_for_its_prefix_typed_inside_input():
    completer = PhraseWordCompleter('add genesis transaction')
    document = Document('add gtransaction', 5)
    complete_event = CompleteEvent(text_inserted=True)

    completions = list(completer.get_completions(document, complete_event))

    assert [Completion('genesis', -1)] == completions


def test_typed_word_is_suggested_for_itself_if_it_is_next_phrase_word():
    completer = PhraseWordCompleter('add genesis transaction')
    document = Document('add genesis transaction')
    complete_event = CompleteEvent(text_inserted=True)

    completions = list(completer.get_completions(document, complete_event))

    assert [Completion('transaction', -11)] == completions


def test_first_phrase_word_is_suggested_for_empty_input():
    completer = PhraseWordCompleter('add genesis transaction')
    document = Document('')
    complete_event = CompleteEvent(completion_requested=True)

    completions = list(completer.get_completions(document, complete_event))

    assert [Completion('add')] == completions


def test_none_is_suggested_for_space_after_all_phrase():
    completer = PhraseWordCompleter('add genesis transaction')
    document = Document('add genesis transaction ')
    complete_event = CompleteEvent(completion_requested=True)

    completions = list(completer.get_completions(document, complete_event))

    assert [] == completions


def test_none_is_suggested_for_word_typed_after_all_phrase():
    completer = PhraseWordCompleter('add genesis transaction')
    document = Document('add genesis transaction new')
    complete_event = CompleteEvent(completion_requested=True)

    completions = list(completer.get_completions(document, complete_event))

    assert [] == completions


def test_none_is_suggested_for_any_input_after_all_phrase_and_space():
    completer = PhraseWordCompleter('add genesis transaction')
    document = Document('add genesis transaction new tr')
    complete_event = CompleteEvent(completion_requested=True)

    completions = list(completer.get_completions(document, complete_event))

    assert [] == completions


def test_first_phrase_word_is_suggested_for_only_space_typed():
    completer = PhraseWordCompleter('add genesis transaction')
    document = Document(' ')
    complete_event = CompleteEvent(text_inserted=True)

    completions = list(completer.get_completions(document, complete_event))

    assert [Completion('add')] == completions


def test_next_phrase_word_is_suggested_for_redundant_space():
    completer = PhraseWordCompleter('add genesis transaction')
    document = Document('  add   genesis  ')
    complete_event = CompleteEvent(text_inserted=True)

    completions = list(completer.get_completions(document, complete_event))

    assert [Completion('transaction')] == completions
