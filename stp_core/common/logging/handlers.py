import logging


class CallbackHandler(logging.Handler):
    def __init__(self, typestr, default_tags, callback, override_tags):
        """
        Initialize the handler.
        """
        super().__init__()
        self.callback = callback
        self.tags = default_tags
        self.update_tags(override_tags or {})
        self.typestr = typestr

    def update_tags(self, override_tags):
        self.tags.update(override_tags)

    def emit(self, record):
        """
        Passes the log record back to the CLI for rendering
        """
        should_cb = None
        attr_val = None
        if hasattr(record, self.typestr):
            attr_val = getattr(record, self.typestr)
            should_cb = bool(attr_val)
        if should_cb is None and record.levelno >= logging.INFO:
            should_cb = True
        if hasattr(record, 'tags'):
            for t in record.tags:
                if t in self.tags:
                    if self.tags[t]:
                        should_cb = True
                        continue
                    else:
                        should_cb = False
                        break
        if should_cb:
            self.callback(record, attr_val)


class CliHandler(CallbackHandler):
    def __init__(self, callback, override_tags=None):
        default_tags = {
            "add_replica": True
        }
        super().__init__(typestr="cli",
                         default_tags=default_tags,
                         callback=callback,
                         override_tags=override_tags)


class DemoHandler(CallbackHandler):
    def __init__(self, callback, override_tags=None):
        default_tags = {
            "add_replica": True
        }
        super().__init__(typestr="demo",
                         default_tags=default_tags,
                         callback=callback,
                         override_tags=override_tags)


class TestingHandler(logging.Handler):
    def __init__(self, tester):
        """
        Initialize the handler.
        """
        super().__init__()
        self.tester = tester

    def emit(self, record):
        """
        Captures a record.
        """
        self.tester(record)
