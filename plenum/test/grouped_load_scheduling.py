import itertools
from xdist.dsession import LoadScheduling

"""
Custom scheduler implementation that prevents running in parallel tests, that belong same class (TestCase).
Custom scheduler support implemented in this PR: https://github.com/pytest-dev/pytest-xdist/pull/89
"""


class GroupedLoadScheduling(LoadScheduling):
    def check_schedule(self, node, duration=0):
        """Maybe schedule new items on the node

        If there are any globally pending nodes left then this will
        check if the given node should be given any more tests.  The
        ``duration`` of the last test is optionally used as a
        heuristic to influence how many tests the node is assigned.
        """
        if node.shutting_down:
            return

        if duration >= 1 and len(self.node2pending[node]) >= 2:
            # seems the node is doing long-running tests
            # and has enough items to continue
            # so let's rather wait with sending new items
            return

        if self.pending:
            self._send_tests(node)

        self.log("num items waiting for node:", len(self.pending))

    def perfect_batch_size(self):
        return max(len(self.pending) // 8, len(self.nodes) * 2)

    def _send_tests(self, node, _):
        node_count = 0
        bsize = self.perfect_batch_size()
        batches = self.batch_generator()

        while node_count < bsize:
            try:
                _, batch = next(batches)
                batch = list(batch)
                for b in batch:
                    self.pending.remove(b)
                self.node2pending[node].extend(batch)
                node.send_runtest_some(batch)
                node_count += len(batch)
            except StopIteration:
                break

    def batch_generator(self):
        def grouper(p):
            name = self.collection[p]
            return '::'.join(name.split('::')[:-1])

        return itertools.groupby(self.pending, key=grouper)
