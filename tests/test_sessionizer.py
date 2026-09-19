"""
tests/test_sessionizer.py
--------------------------
TODO: Unit tests for the sessionization logic.

Suggested test cases:
 - Single user, single session: all events share the same session_id.
 - Single user, gap > 1800 s: events split into 2 distinct session_ids.
 - First event always starts a new session (null diff -> flag = 1).
 - computed_session_id format is "{user_id}_{session_index}".
 - Multiple users are sessionized independently (no cross-user contamination).
"""

import pytest


class TestSessionizer:
    def test_single_session_no_gap(self):
        # TODO
        pass

    def test_session_split_on_timeout(self):
        # TODO
        pass

    def test_first_event_always_new_session(self):
        # TODO
        pass

    def test_session_id_format(self):
        # TODO
        pass

    def test_multiple_users_independent(self):
        # TODO
        pass
