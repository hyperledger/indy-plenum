#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
import argparse
import sys
import ast
import re


class SetupParserError(Exception):
    pass


def extractRequires(src, dumpfname=None):
    # https://docs.python.org/3.5/distutils/setupscript.html#relationships-between-distributions-and-packages
    re_package_name = re.compile("([^><=!]+)")

    tree = ast.parse(src)

    references = {}
    assignments = []

    requires = {
        'setup_requires': {},
        'install_requires': {},
        'tests_require': {},
        'extras_require': {}
    }

    class CutAssignments(ast.NodeTransformer):
        def visit_Assign(self, node):
            return None if node in assignments else node

    class CutRequires(ast.NodeTransformer):
        def _process_package(self, node, dest):
            assert(type(node) == ast.Str)
            m = re_package_name.match(node.s)
            dest['' if m is None else m.group(0)] = 1

        def _process_list(self, node, dest):
            assert(type(node) == ast.List)
            for pack in node.elts:
                self._process_package(pack, dest)

        def _process_reference(self, node, dest):
            assert(type(node) == ast.Name)

            if type(node.ctx) != ast.Load:
                raise SetupParserError(
                    "ast.Load context is "
                    "expected only, got \'{}\'".format(type(node.ctx)))
            elif node.id not in references:
                raise SetupParserError(
                    "reference \'{}\' is unknown".format(node.id))
            else:
                ref_name = node.id
                ref = references[node.id]

                if type(ref["node"].ctx) != ast.Store:
                    raise SetupParserError(
                        "reference \'{}\' has unsupported ctx type: "
                        "\'{}\'".format(ref_name, type(ref["node"].ctx)))
                elif type(ref["value"]) not in (ast.List, ast.Str):
                    raise SetupParserError(
                        "reference \'{}\' has unsupported value type: "
                        "\'{}\'".format(ref_name, type(ref["value"])))
                else:
                    assignments.append(ref["assign"])
                    self._process_value(ref["value"], dest)

        def _process_value(self, node, dest):
            if type(node) == ast.List:
                self._process_list(node, dest)
            elif type(node) == ast.Name:
                self._process_reference(node, dest)
            elif type(node) == ast.Str:
                self._process_package(node, dest)
            else:
                raise SetupParserError(
                    "value of type '{}' is unexpected".format(type(node)))

        def _process_extras(self, node, dest):
            assert(type(node) == ast.Dict)
            for (index, key) in enumerate(node.keys):
                if key not in dest:
                    dest[key.s] = {}
                self._process_value(node.values[index], dest[key.s])

        def visit_keyword(self, node):
            if node.arg in requires:
                try:
                    if node.arg == 'extras_require':
                        self._process_extras(node.value, requires[node.arg])
                    else:
                        self._process_value(node.value, requires[node.arg])
                except SetupParserError as e:
                    print("WARNING: failed to parse keyword \'{}\' [{}], "
                          "ignored".format(node.arg, e))
                    return node
                else:
                    return None     # remove it from code tree
            else:
                return node

    for ch in ast.iter_child_nodes(tree):
        if type(ch) == ast.Expr and type(ch.value) == ast.Call and \
                type(ch.value.func) == ast.Name and \
                ch.value.func.id == "setup":
            CutRequires().visit(ch.value)
        # for now process single only assignment
        elif type(ch) == ast.Assign and len(ch.targets) == 1 and \
                type(ch.targets[0]) == ast.Name:
            target = ch.targets[0]
            references[target.id] = {
                "assign": ch,
                "node": target,
                "value": ch.value
            }

    CutAssignments().visit(tree)

    if dumpfname is not None:
        with open(dumpfname, "w+") as f:
            f.write(ast.dump(tree, annotate_fields=False))

    return (ast.dump(tree, annotate_fields=False), requires)


def compare(src1, src2, dump=None):
    (setup_orig, requires_orig) = extractRequires(
        src1, "{}.1".format(dump) if dump is not None else None)
    (setup_modif, requires_modif) = extractRequires(
        src2, "{}.2".format(dump) if dump is not None else None)
    return (0 if (setup_orig == setup_modif) and
            (requires_orig == requires_modif) else 1)


def main(file1, file2):
    parser = argparse.ArgumentParser(
        description='setup.py files comparer',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument('old', metavar='old',
                        help='Original setup.py')

    parser.add_argument('new', metavar='new',
                        help='Modified setup.py')

    parser.add_argument("--dump", default=None,
                        help="Dump file basename")

    args = parser.parse_args()

    with open(args.old) as f:
        src1 = f.read()

    with open(args.new) as f:
        src2 = f.read()

    return compare(src1, src2, args.dump)


if __name__ == '__main__':
    sys.exit(main(sys.argv[1], sys.argv[2]))
