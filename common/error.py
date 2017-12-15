def error(msg: str, exc_type: Exception=Exception) -> Exception:
    """
    Wrapper to get around Python's distinction between statements and expressions
    Can be used in lambdas and expressions such as: a if b else error(c)

    :param msg: error message
    :param exc_type: type of exception to raise
    """
    raise exc_type(msg)
