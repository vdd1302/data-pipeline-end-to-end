"""M8-only authoritative route selection from the canonical M5 JSON registry.

Do not infer verification from a hard-coded historical URL or a public URL
candidate. This loader performs local file I/O only (no Selenium/network/DB).
"""
from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import urlsplit


class M8RegistryError(ValueError):
    """The canonical registry is missing, ambiguous or unsafe for M8."""


def read_verified_registry(path: Path) -> dict[tuple[str, str], str]:
    path = Path(path)
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 1024 * 1024:
        raise M8RegistryError('Canonical M5 route registry missing or unsafe.')
    try:
        data = json.loads(path.read_text(encoding='utf-8-sig'))
    except (OSError, ValueError, UnicodeError) as exc:
        raise M8RegistryError('Cannot read canonical M5 route registry.') from exc
    if not isinstance(data, dict) or data.get('version') != 1 or not isinstance(data.get('routes'), list):
        raise M8RegistryError('Unexpected canonical M5 registry structure.')
    verified = {}
    observed = set()
    for row in data['routes']:
        if not isinstance(row, dict):
            raise M8RegistryError('Malformed M5 registry row.')
        key = row.get('position_id'), row.get('location_id')
        if (not all(isinstance(x, str) and x for x in key)
                or key[1] not in {'ha_noi', 'ho_chi_minh'}):
            raise M8RegistryError('Malformed M5 scope identity.')
        if key in observed:
            raise M8RegistryError('Duplicate M5 scope in canonical registry.')
        observed.add(key)
        if row.get('status') != 'live_verified':
            continue
        url, evidence = row.get('route'), row.get('evidence')
        if not isinstance(url, str) or not isinstance(evidence, dict):
            raise M8RegistryError('M5 verified entry lacks URL or evidence.')
        try:
            parsed = urlsplit(url)
            city_slug = 'ha-noi' if key[1] == 'ha_noi' else 'ho-chi-minh'
            city_code = 'l1' if key[1] == 'ha_noi' else 'l2'
            valid_url = (parsed.scheme == 'https' and parsed.netloc == 'www.topcv.vn'
                         and not parsed.query and not parsed.fragment
                         and parsed.path.startswith('/tim-viec-lam-')
                         and f'-tai-{city_slug}-{city_code}cr257' in parsed.path)
        except ValueError:
            valid_url = False
        valid_evidence = (evidence.get('scope_verified') is True
                          and evidence.get('all_cards_city_verified') is True
                          and type(evidence.get('parsed_cards')) is int
                          and evidence['parsed_cards'] > 0)
        if not valid_url or not valid_evidence:
            raise M8RegistryError('Invalid URL or proof for verified M5 scope: ' + '/'.join(key))
        verified[key] = url
    return verified
