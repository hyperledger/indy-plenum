import random
from collections import defaultdict, Iterable
from typing import Dict

import gc

import time

from plenum.common.metrics_collector import MetricsCollector, MetricsName
from stp_core.common.log import getlogger

logger = getlogger()


class GcObject:
    def __init__(self, obj):
        self.obj = obj
        self.referents = [id(ref) for ref in gc.get_referents(obj)]
        self.referrers = set()


class GcObjectTree:
    def __init__(self):
        self.objects = {id(obj): GcObject(obj) for obj in gc.get_objects()}
        for obj_id, obj in self.objects.items():
            for ref_id in obj.referents:
                if ref_id in self.objects:
                    self.objects[ref_id].referrers.add(obj_id)

    def report_top_obj_types(self, num=50):
        stats = defaultdict(int)
        for obj in self.objects.values():
            stats[type(obj.obj)] += 1
        for k, v in sorted(stats.items(), key=lambda kv: -kv[1])[:num]:
            logger.info("    {}: {}".format(k, v))

    def report_top_collections(self, num=10):
        fat_objects = [(o.obj, len(o.referents)) for o in self.objects.values()]
        fat_objects = sorted(fat_objects, key=lambda kv: -kv[1])[:num]

        logger.info("Top big collections tracked by garbage collector:")
        for obj, count in fat_objects:
            logger.info("    {}: {}".format(type(obj), count))
            self.report_collection_owners(obj)
            self.report_collection_items(obj)

    def report_collection_owners(self, obj):
        referrers = {ref_id for ref_id in self.objects[id(obj)].referrers if ref_id in self.objects}
        for _ in range(3):
            self.add_super_referrerrs(referrers)
        referrers = {type(self.objects[ref_id].obj) for ref_id in referrers}

        logger.info("        Referrers:")
        for v in referrers:
            logger.info("            {}".format(v))

    def add_super_referrerrs(self, referrers):
        for ref_id in list(referrers):
            for sup_ref_id in self.objects[ref_id].referrers:
                if sup_ref_id in self.objects:
                    referrers.add(sup_ref_id)

    def report_collection_items(self, obj):
        if not isinstance(obj, Iterable):
            return
        tmp_list = list(obj)
        samples = random.sample(tmp_list, 3)

        logger.info("        Samples:")
        for k in samples:
            if isinstance(obj, Dict):
                logger.info("            {} : {}".format(repr(k), repr(obj[k])))
            else:
                logger.info("            {}".format(repr(k)))

    def cleanup(self):
        del self.objects


class GcTimeTracker:
    def __init__(self, metrics: MetricsCollector):
        self._metrics = metrics
        self._timestamps = {}
        gc.callbacks.append(self._gc_callback)

    def _gc_callback(self, action, info):
        gen = info['generation']

        collected = info.get('collected', 0)
        if collected > 0:
            self._metrics.add_event(MetricsName.GC_TOTAL_COLLECTED_OBJECTS, collected)
            self._metrics.add_event(MetricsName.GC_GEN0_COLLECTED_OBJECTS + gen, collected)

        uncollectable = info.get('uncollectable', 0)
        if uncollectable > 0:
            self._metrics.add_event(MetricsName.GC_UNCOLLECTABLE_OBJECTS, uncollectable)

        if action == 'start':
            self._timestamps[gen] = time.perf_counter()
        else:
            start = self._timestamps.get(gen)
            if start is None:
                return
            elapsed = time.perf_counter() - start
            self._metrics.add_event(MetricsName.GC_GEN0_TIME + gen, elapsed)
            self._timestamps[gen] = None
