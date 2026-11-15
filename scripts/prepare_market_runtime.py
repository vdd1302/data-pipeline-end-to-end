"""Generate private local runtime settings once; never print secrets."""
from pathlib import Path
import secrets
from dotenv import dotenv_values
from psycopg.conninfo import conninfo_to_dict, make_conninfo

ROOT = Path(__file__).resolve().parents[1]


def main():
    path = ROOT / '.env.market'
    if path.exists():
        print('Existing .env.market preserved.')
        return
    raw = dotenv_values(ROOT / '.env').get('TOPCV_RAW_DATABASE_URL')
    if not raw:
        raise RuntimeError('Configure TOPCV_RAW_DATABASE_URL in .env first')
    raw_settings = conninfo_to_dict(raw)
    if raw_settings.get('host') in ('localhost', '127.0.0.1', '::1'):
        raw_settings['host'] = 'host.docker.internal'
    owner = secrets.token_hex(24)
    reader = secrets.token_hex(24)
    values = {
        'MARKET_DB_PASSWORD': owner,
        'MARKET_READER_PASSWORD': reader,
        'MARKET_JWT_SECRET': secrets.token_hex(32),
        'TOPCV_DW_DATABASE_URL': f'postgresql://market_owner:{owner}@127.0.0.1:55440/market',
        'TOPCV_RAW_CONTAINER_URL': make_conninfo(**raw_settings),
        'MARKET_CONTAINER_DW_URL': f'postgresql://market_owner:{owner}@market-db:5432/market',
        'MARKET_READER_URL': f'postgresql+psycopg2://market_reader:{reader}@host.docker.internal:55440/market',
    }
    with path.open('x', encoding='utf-8', newline='\n') as stream:
        for key, value in values.items():
            if '\n' in value or "'" in value:
                raise ValueError('Unsupported connection configuration quoting')
            stream.write(key + "='" + value + "'\n")
    print('Created .env.market with private random runtime credentials.')


if __name__ == '__main__':
    main()
