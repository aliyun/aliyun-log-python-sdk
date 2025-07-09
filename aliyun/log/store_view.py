#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

class StoreViewStore:
    def __init__(self, project_name, store_name, query=None):
        self._project_name = project_name
        self._store_name = store_name
        self._query = query

    @property
    def project_name(self):
        """
        Get the project name of the store view store

        :return: string, the project of the store view store
        """
        return self._project_name

    @project_name.setter
    def project_name(self, project_name):
        """
        Set the project name of the store view store

        :param project_name: string, the project of the store view store
        """
        self._project_name = project_name

    @property
    def store_name(self):
        """
        Get the name of the store view store

        :return: string, the name of the store view store
        """
        return self._store_name

    @store_name.setter
    def store_name(self, store_name):
        """
        Set the name of the store view store

        :param store_name: string, the name of the store view store
        """
        self._store_name = store_name

    @property
    def query(self):
        """
        Get the query of the store view store

        :return: string, the query of the store view store
        """
        return self._query

    @query.setter
    def query(self, query):
        """
        Set the query of the store view store

        :param query: string, the query of the store view store, the query is optional
        """
        self._query = query

    def _to_json_dict(self):
        if self._query is not None:
            return {
                "project": self._project_name,
                "storeName": self._store_name,
                "query": self._query,
            }
        return {
            "project": self._project_name,
            "storeName": self._store_name,
        }

    @classmethod
    def _from_json_dict(cls, json_data):
        project_name = json_data.get("project")
        store_name = json_data.get("storeName")
        query = json_data.get("query")
        return cls(project_name, store_name, query)


class StoreView:
    def __init__(self, name, store_type, stores=[]):
        self._name = name
        self._store_type = store_type
        self._stores = stores

    @property
    def name(self):
        """
        Get the name of the store view

        :return: string, the name of the store view
        """
        return self._name

    @name.setter
    def name(self, name):
        """
        Set the name of the store view

        :param name: string, the name of the store view
        """
        self._name = name

    @property
    def store_type(self):
        """
        Get the type of the store view

        :return: string, the type of the store view, could be "logstore" or "metricstore"
        """
        return self._store_type

    @store_type.setter
    def store_type(self, store_type):
        """
        Set the type of the store view

        :param store_type: string, the type of the store view, could be "logstore" or "metricstore"
        """
        self._store_type = store_type

    @property
    def stores(self):
        """
        Get the stores of the store view

        :return: list, the unioned stores of the store view
        """
        return self._stores

    @stores.setter
    def stores(self, stores):
        """
        Set the stores of the store view

        :param stores: list, the unioned stores of the store view
        """
        self._stores = stores

    def _to_json_dict(self):
        return {
            "name": self._name,
            "storeType": self._store_type,
            "stores": [store._to_json_dict() for store in (self._stores or [])],
        }

    @classmethod
    def _from_json_dict(cls, json_data):
        name = json_data.get("name")
        store_type = json_data.get("storeType")
        stores = [
            StoreViewStore._from_json_dict(store)
            for store in json_data.get("stores", [])
        ]
        return cls(name, store_type, stores)
