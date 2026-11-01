"""Replay a committed canonical journal, including controls and clocks."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from recognition.models import MarketEvent
from recognition.provenance import file_sha256
from .config import RuntimeConfig, canonical
from .runner import Runner, RecoveryBlocked, engine_snapshot_sha256


def derived_hash(store):
    return store.derived_hash()


def replay_journal(path, directory, configuration, *, expected_identity=None, committed_cursor=None, expected_journal_sha256=None):
    """Preserve source order and decision times; pending originals remain pending.

    New run identity is deliberate. Derived records and account values are comparable
    across run IDs because run IDs are envelope metadata, not recognition inputs.
    """
    path=Path(path)
    input_hash=file_sha256(path)
    config=RuntimeConfig.load(**configuration)
    runner=Runner(directory,config)
    try:
        if runner.store.latest_seq(): raise ValueError('journal replay requires an empty run directory')
        if expected_identity and runner._identity!=expected_identity:
            raise RecoveryBlocked('journal belongs to a different algorithm/configuration/execution version')
        manifest_path=path.parent/'manifest.json'
        if manifest_path.is_file():
            manifest=json.loads(manifest_path.read_text())
            declared=next((r['sha256'] for r in manifest['files'] if r['path']==path.name),None)
            if declared is None or declared!=input_hash: raise ValueError('export manifest does not match journal')
        count=0
        with path.open(encoding='utf-8') as handle:
            for line in handle:
                if not line.strip(): continue
                row=json.loads(line)
                if row['seq']!=count+1 or row['kind'] not in {'market','backfill','control','clock','source_status','label'}:
                    raise ValueError('journal must have contiguous canonical sequence and supported event kinds')
                if row['kind'] in {'market','backfill','clock'}: MarketEvent.from_dict(row['payload'])
                seq,created=runner.store.append(row['kind'],row['source'],row['payload'],row.get('event_id'))
                if not created or seq!=row['seq']: raise ValueError('journal identity is duplicated or sequence changed')
                if committed_cursor is None or seq<=committed_cursor: runner.drain()
                count=seq
        if file_sha256(path)!=input_hash: raise ValueError('journal changed during replay')
        if expected_journal_sha256 and runner.journal_hash()!=expected_journal_sha256:
            raise ValueError('journal content differs from the original run report')
        if committed_cursor is not None and runner.cursor!=committed_cursor: raise ValueError('committed cursor is absent from journal')
        runner.checkpoint()
        result={'events':count,'processed_cursor':runner.cursor,'pending_retained':count-runner.cursor,
                'input_sha256':input_hash,'derived_sha256':derived_hash(runner.store),
                'account':runner.paper.account(),'engine_sha256':engine_snapshot_sha256(runner.engine),
                'paper_sha256':hashlib.sha256(canonical(runner.paper.snapshot()).encode()).hexdigest(),
                'replay_origin_mode':config.mode,'execution':'paper_only','market_validation':'not_run'}
        runner.store.set_meta('journal_replay',result)
        runner.store.set_meta('source',{'name':'journal_replay','mode':'replay','origin_mode':config.mode,'input':str(path.resolve()),'input_sha256':input_hash,'source_scope':'preserved_original_journal'})
        return result
    finally: runner.close()
