import os


def cleanLines(source, lineSep=os.linesep):
    """
    :param source: some iterable source (list, file, etc)
    :param lineSep: string of separators (chars) that must be removed
    :return: list of non empty lines with removed separators
    """
    stripped = (line.strip(lineSep) for line in source)
    return (line for line in stripped if len(line) != 0)
