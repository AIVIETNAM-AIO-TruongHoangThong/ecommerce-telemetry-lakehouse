"""
tests/conftest.py
-----------------
Shared pytest fixtures for the test suite.

TODO [Unit Testing]:
 - Add a local SparkSession fixture (scope='session') so tests don't
    re-create Spark for every test function.
 - Add sample DataFrame fixtures for schema and sessionization tests.
"""

import pytest


@pytest.fixture(scope="session")
def spark():
    """
    Provide a local SparkSession for unit tests.

    TODO [Unit Testing]:
    1. Import SparkSession.
    2. Build with master='local[2]', appName='test-suite'.
    3. yield spark - SparkSession is reused across the whole test session.
    4. Call spark.stop() after yield.
    """
    raise NotImplementedError("Implement the spark fixture.")
    yield  # placeholder


@pytest.fixture()
def sample_raw_events(spark):
    """
    Return a small DataFrame mimicking raw REES46 event records.

    TODO [Unit Testing]:
    Build a DataFrame from a hardcoded list of Row objects covering:
 - At least 2 users.
 - Events spanning multiple sessions (gap > 1800 s between some events).
 - At least one purchase event and one corrupt record (null user_id).
    """
    raise NotImplementedError("Implement sample_raw_events fixture.")
