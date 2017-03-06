import re

from prompt_toolkit.completion import Completer
from prompt_toolkit.contrib.completers.base import WordCompleter


class PhraseWordCompleter(Completer):

    class Rule:
        def __init__(self, prefix_regex, completer):
            self.prefix_regex = prefix_regex
            self.completer = completer

    def __init__(self, phrase):
        """Autocompletion for a current word in a given ordered list of words.
        :param phrase: Phrase consisting of multiple words.
        """
        self.phrase = phrase
        words = phrase.split()
        self.rules = []
        prev_words = '^\s*'
        any_last_word = '\w*$'
        for word in words:
            self.rules.append(self.Rule(re.compile(prev_words + any_last_word),
                                        WordCompleter([word])))
            prev_words += word + '\s+'

    def get_completions(self, document, complete_event):
        text_before_cursor = document.text_before_cursor
        for rule in self.rules:
            if rule.prefix_regex.match(text_before_cursor):
                yield from rule.completer.get_completions(document,
                                                          complete_event)
                break
