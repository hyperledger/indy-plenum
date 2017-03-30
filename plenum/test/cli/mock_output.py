from prompt_toolkit.output import Output


class MockOutput(Output):
    def __init__(self, recorder=None):
        self.writes = []
        self.recorder = recorder

    def fileno(self):
        raise NotImplementedError

    def cursor_up(self, amount):
        raise NotImplementedError

    def erase_screen(self):
        raise NotImplementedError

    def hide_cursor(self):
        raise NotImplementedError

    def set_attributes(self, attrs):
        pass

    def enable_mouse_support(self):
        raise NotImplementedError

    def clear_title(self):
        raise NotImplementedError

    def quit_alternate_screen(self):
        raise NotImplementedError

    def enable_autowrap(self):
        pass

    def erase_end_of_line(self):
        raise NotImplementedError

    def cursor_backward(self, amount):
        raise NotImplementedError

    def flush(self):
        pass

    def disable_autowrap(self):
        raise NotImplementedError

    def erase_down(self):
        raise NotImplementedError

    def cursor_forward(self, amount):
        raise NotImplementedError

    def cursor_goto(self, row=0, column=0):
        raise NotImplementedError

    def disable_mouse_support(self):
        raise NotImplementedError

    def show_cursor(self):
        raise NotImplementedError

    def cursor_down(self, amount):
        raise NotImplementedError

    def enter_alternate_screen(self):
        raise NotImplementedError

    def set_title(self, title):
        raise NotImplementedError

    def write_raw(self, data):
        raise NotImplementedError

    def write(self, data):
        self.writes.append(data)
        if self.recorder:
            self.recorder.write(data)

    def reset_attributes(self):
        pass

    def scroll_buffer_to_prompt(self):
        pass
