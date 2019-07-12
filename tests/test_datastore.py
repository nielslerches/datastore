import unittest
import warnings

from datastore import DataStore


class DataStoreTest(unittest.TestCase):
    def setUp(self):
        self.datastore = DataStore(
            initial_dataset=[
                {
                    'id': 1,
                    'name': 'Nemeziz',
                },
                {
                    'id': 2,
                    'name': 'Copa Mundial',
                },
            ],
        )

    def test_get_by_eq(self):
        self.assertListEqual(
            self.datastore.get(id=2),
            self.datastore.get(id__eq=2),
        )
        self.assertListEqual(
            self.datastore.get(id=2),
            [{'id': 2, 'name': 'Copa Mundial'}],
        )

    def test_get_by_neq(self):
        self.assertListEqual(
            self.datastore.get(id__neq=2),
            [{'id': 1, 'name': 'Nemeziz'}],
        )

    def test_get_by_lt(self):
        return self.assertListEqual(
            self.datastore.get(id__lt=2),
            [{'id': 1, 'name': 'Nemeziz'}],
        )

    def test_get_by_lte(self):
        return self.assertListEqual(
            self.datastore.get(id__lte=2),
            [{'id': 1, 'name': 'Nemeziz'}, {'id': 2, 'name': 'Copa Mundial'}],
        )

    def test_get_by_gt(self):
        return self.assertListEqual(
            self.datastore.get(id__gt=1),
            [{'id': 2, 'name': 'Copa Mundial'}],
        )

    def test_get_by_gte(self):
        return self.assertListEqual(
            self.datastore.get(id__gte=1),
            [{'id': 1, 'name': 'Nemeziz'}, {'id': 2, 'name': 'Copa Mundial'}],
        )

    def test_successful_add(self):
        datastore = DataStore()
        datastore.add({'name': 'John Doe'})

    def test_unsucessful_add(self):
        datastore = DataStore(indices_config={('name',): True})
        datastore.add({'name': 'John Doe'})
        with self.assertWarns(UserWarning):
            datastore.add({'name': 'John Doe'})
        self.assertTrue(len(datastore.dataset) == 1)
