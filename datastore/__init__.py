import warnings

from collections import defaultdict
from uuid import uuid4


def partially_masks(iterable1, iterable2):
    for item in iterable1:
        if item in iterable2:
            return True

    return False


class DataStore:
    comparisons = {
        'eq': lambda a, b: a == b,
        'neq': lambda a, b: a != b,
        'lt': lambda a, b: a < b,
        'lte': lambda a, b: a <= b,
        'gt': lambda a, b: a > b,
        'gte': lambda a, b: a >= b,
        'startswith': lambda a, b: a.startswith(b),
        'endswith': lambda a, b: a.endswith(b),
        'in': lambda a, b: a in b,
        'contains': lambda a, b: b in a,
    }

    def __init__(self, indices_config=None, initial_dataset=None):
        self.indices_config = indices_config.copy() if indices_config is not None or {}
        self.dataset = {}
        self.indices = {
            index_key: defaultdict(list)
            for index_key
            in self.indices_config.keys()
        }

        self.indices_config = {
            tuple(sorted(index_key, key=lambda item: item[0])): unique
            for index_key, unique
            in self.indices_config.items()
        }

        self.indices = {
            tuple(sorted(index_key, key=lambda item: item[0])): index
            for index_key, index
            in self.indices.items()
        }

        indices_config_overrides = []
        for index_key1, unique1 in self.indices_config.items():
            for index_key2, unique2 in self.indices_config.items():
                if len(index_key2) > len(index_key1) and unique1 and not unique2 and partially_masks(index_key2, index_key1):
                    warnings.warn('{0!r} partially masks {1!r}, while {1!r} has a unique constraint. overriding unique constraint for {0!r} with True'.format(index_key2, index_key1))
                    indices_config_overrides.append((index_key2, True))

        for index_key, unique in indices_config_overrides:
            self.indices_config[index_key] = unique

        for entry in (initial_dataset or []):
            self.add(entry=entry)

    def add(self, entry):
        entry_id = str(uuid4())

        index_additions = []
        ok = True
        for index_key, unique in (
            (index_key, unique)
            for index_key, unique
            in self.indices_config.items()
            if all(
                (key in entry)
                for key
                in index_key
            )
        ):
            index_entry_key = tuple(
                entry[entry_value_key]
                for entry_value_key
                in index_key
            )

            if unique and index_entry_key in self.indices[index_key]:
                warnings.warn('{!r} would break unique constraint in indices[{!r}]'.format(index_entry_key, index_key))
                if ok:
                    warnings.warn('skipping entry entirely')
                    ok = False
                continue
            index_additions.append((index_key, index_entry_key))

        if ok:
            self.dataset[entry_id] = entry
            for index_key, index_entry_key in index_additions:
                self.indices[index_key][index_entry_key].append(entry_id)

    def get(self, **kwargs):
        comparisons = []
        for key, value in kwargs.items():
            for comparison_key in self.comparisons.keys():
                if key.endswith('__' + comparison_key):
                    entry_value_key = key.rsplit('__', 1)[0]
                    comparisons.append((entry_value_key, comparison_key, value))
                    break
            else:
                comparisons.append((key, 'eq', value))

        index_key = tuple(zip(*sorted(comparisons, key=lambda item: item[0])))[0]
        if index_key in self.indices:
            entry_ids = []
            for index_entry_key, index_entry_ids in self.indices[index_key].items():
                if all(
                    self.comparisons[comparison_key](index_entry_key[i], value)
                    for i, entry_value_key, comparison_key, value
                    in (
                        (i,) + item
                        for i, item
                        in enumerate(comparisons)
                    )
                ):
                    entry_ids.extend(index_entry_ids)

            return [
                self.dataset[entry_id]
                for entry_id
                in entry_ids
            ]

        return [
            entry
            for entry
            in self.dataset.values()
            if all(
                self.comparisons[comparison_key](entry[entry_value_key], value)
                for entry_value_key, comparison_key, value
                in comparisons
            )
        ]
