#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Configuration helpers for OrigenToGraph.

All values are read from environment variables (loaded by the root main.py
via python-dotenv before any subproject module is imported).
"""

import os


def get_neo4j_config() -> dict:
    """Return Neo4j connection parameters from environment."""
    return {
        "uri":      os.getenv("NEO4J_HOST", "bolt://localhost:7687"),
        "user":     os.getenv("NEO4J_USER", "neo4j"),
        "password": os.getenv("NEO4J_PASS", "password"),
    }


def get_elastic_config() -> dict:
    """Return Elasticsearch connection parameters from environment."""
    return {
        "host":     os.getenv("ELASTIC_HOST", "localhost"),
        "port":     int(os.getenv("ELASTIC_PORT", "9200")),
        "username": os.getenv("ELASTIC_USERNAME", "elastic"),
        "password": os.getenv("ELASTIC_PASSWORD", "password"),
    }


def get_hive_config() -> dict:
    """Return Hive / HiveServer2 connection parameters from environment."""
    return {
        "host":           os.getenv("HIVE_HOST", "localhost"),
        "username":       os.getenv("HIVE_USERNAME"),
        "password":       os.getenv("HIVE_PASSWORD"),
        "port":           int(os.getenv("HIVE_PORT", "10000")),
        "database":       os.getenv("HIVE_DATABASE", "default"),
        "auth_mechanism": os.getenv("HIVE_AUTH", "PLAIN"),
        "timeout":        int(os.getenv("HIVE_TIMEOUT", "600")),
    }
