"""Build a single-root, secret-free ZIP containing only reviewed project source.

Usage: python -m scripts.build_clean_release --output ../Project1_VuDinhDong_Release.zip
"""
from __future__ import annotations
import argparse
from hashlib import sha256
import json
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED

ROOT = Path(__file__).resolve().parents[1]
ALLOWED_FOLDERS = frozenset({'airflow','bi','config','docs','scripts','sql','src','tests','tools'})
ROOT_FILES = frozenset({'README.md','requirements.txt','.gitignore','.env.example'})
BANNED_PATH_PARTS = frozenset({
    '.venv', '.venv312', 'venv', 'browser_profile', '__pycache__', '.pytest_cache',
    '.git', 'artifacts', 'logs', 'runtime', '.backups', 'debug', 'secrets',
})
BANNED_NAMES = frozenset({'.env','.env.market','Cookies','Login Data','Web Data','History'})
TEXT_EXTENSIONS = frozenset({'.py','.sql','.yaml','.yml','.json','.md','.txt','.example','.gitignore'})


def included(path: Path, root: Path=ROOT) -> bool:
    rel = path.relative_to(root)
    if not path.is_file() or any(p in BANNED_PATH_PARTS for p in rel.parts):
        return False
    if path.name in BANNED_NAMES or path.name.startswith('.env.') and path.name != '.env.example':
        return False
    if len(rel.parts)==1:
        return path.name in ROOT_FILES
    if rel.parts[0] not in ALLOWED_FOLDERS:
        return False
    return path.suffix.lower() in TEXT_EXTENSIONS or path.name in ('Dockerfile.market','.env.example','.gitignore')


def build(output: Path, root: Path=ROOT) -> dict:
    output = output.resolve()
    if root.resolve() in output.parents:
        raise ValueError('Output must be outside project root')
    sources=sorted((p for p in root.rglob('*') if included(p,root)),key=lambda p:p.relative_to(root).as_posix())
    if len(sources)<40 or not (root/'README.md') in sources:
        raise ValueError('Source archive appears incomplete')
    output.parent.mkdir(parents=True,exist_ok=True)
    with ZipFile(output, 'w', compression=ZIP_DEFLATED, compresslevel=9) as z:
        for p in sources:
            name='Project1_VuDinhDong/'+p.relative_to(root).as_posix()
            if p.stat().st_size > 3_000_000:
                raise ValueError('Suspiciously large release file: '+name)
            z.write(p,name)
    with ZipFile(output) as z:
        names=z.namelist()
        if z.testzip() is not None or len(names)!=len(set(names)):
            raise RuntimeError('Zip archive has failed CRC or duplicate names')
        if any(not n.startswith('Project1_VuDinhDong/') for n in names):
            raise RuntimeError('Unexpected archive root')
    return {'filename':str(output), 'sha256':sha256(output.read_bytes()).hexdigest(),
            'file_count':len(sources),'archive_crc':'PASS','sensitive_paths':'EXCLUDED'}


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args(argv)
    print(json.dumps(build(args.output),indent=2,ensure_ascii=False))

if __name__=='__main__':
    main()
