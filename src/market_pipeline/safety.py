"""Fail-closed authorization gate for live source network collection.

No configuration/fixture/route verification implies permission to crawl a site.
The operator must independently confirm a permitted source contract.
"""
from __future__ import annotations

import os


def require_source_approval(environ=None):
    env = os.environ if environ is None else environ
    if (env.get('TOPCV_LIVE_COLLECTION_APPROVED') != 'YES' or
            not str(env.get('TOPCV_SOURCE_AUTHORIZATION_REFERENCE') or '').strip()):
        raise PermissionError(
            'Live TopCV collection is disabled. Obtain authorization, then set '
            'TOPCV_LIVE_COLLECTION_APPROVED=YES and a non-secret '
            'TOPCV_SOURCE_AUTHORIZATION_REFERENCE. Offline fixture tests remain available.'
        )
