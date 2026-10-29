"""使用原基准入口交错运行冻结的 RESET 修复前后二进制。"""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

root = Path.cwd()
out = root / 'build/performance-20261029/reset-rpc-ab-v182'
manifest = json.loads((out / 'manifest.json').read_text())
sys.path.insert(0, str(root / 'bench'))
from run_rpc_repeatability import PROFILE_SPECS, summarize
from run_rpc_scalability import clean_result, exact_wall_signals

assert manifest['compilation_profiles_equivalent']
env = dict(os.environ, TMPDIR=str(root / 'build/test-tmp'),
           TMP=str(root / 'build/test-tmp'), TEMP=str(root / 'build/test-tmp'),
           XMAKE_TMPDIR=str(root / 'build/test-tmp'), PYTHONDONTWRITEBYTECODE='1')
identity = root / 'build/performance-20261029/tls-zstd-hour-3600s'
for name in ('ca.pem', 'server.pem', 'server.key'):
    assert (identity / name).is_file()
all_cases = []
events = []

def run(kind, mode, stage, trial=0):
    spec = manifest['binaries'][kind]
    binary = Path(spec['binary'])
    assert hashlib.sha256(binary.read_bytes()).hexdigest() == spec['sha256']
    checkout = Path(spec['checkout'])
    destination = out / f'{stage}-{mode}-{kind}-{trial}.jsonl'
    assert not destination.exists()
    runner = 'run_rpc_bench.py' if stage == 'smoke' else 'run_rpc_repeatability.py'
    command = [sys.executable, str(checkout / 'bench' / runner),
               '--binary', str(binary), '--output', str(destination),
               '--label', f'reset-scan-{mode}-{kind}-{stage}-{trial}']
    if stage == 'smoke':
        command += ['--smoke', '--scenario', 'small']
    else:
        command += ['--trials', '1', '--rate', '40000', '--min-arrival-ms', '250']
    if mode == 'tls-zstd':
        command += ['--tls', '--compression', 'zstd',
                    '--tls-ca', str(identity / 'ca.pem'),
                    '--tls-cert', str(identity / 'server.pem'),
                    '--tls-key', str(identity / 'server.key')]
    start = time.monotonic()
    event = dict(kind=kind, mode=mode, stage=stage, trial=trial,
                 command=command, loadavg_before=os.getloadavg())
    with destination.with_suffix('.log').open('w') as log:
        result = subprocess.run(command, env=env, cwd=checkout,
                                stdout=log, stderr=subprocess.STDOUT, timeout=600)
    event.update(exit_code=result.returncode, elapsed_s=round(time.monotonic()-start, 3),
                 loadavg_after=os.getloadavg())
    events.append(event)
    (out / 'execution.json').write_text(json.dumps(events, indent=2)+'\n')
    if result.returncode:
        raise RuntimeError(str(destination.with_suffix('.log')))
    rows = [json.loads(line) for line in destination.read_text().splitlines()]
    meta = rows[0]
    assert meta['binary_sha256'] == spec['sha256']
    assert meta['git_revision'] == spec.get('metadata_source', spec['runtime_source'])
    assert meta['affinity'] == manifest['affinity']
    assert bool(meta['tls_enabled']) == (mode == 'tls-zstd')
    assert meta['compression'] == ('zstd' if mode == 'tls-zstd' else 'none')
    if stage == 'smoke':
        assert len(rows) == 4 and all(r.get('type') != 'failure' for r in rows)
        print(f'smoke {mode} {kind}: 3/3 closed cases passed', flush=True)
        return
    cases = [r for r in rows if r['type'] == 'repeatability_case']
    assert len(cases) == 3
    for r in cases:
        c = r['case']
        assert c['rate_rps'] == 40000 and c['requests'] == 10000 and c['generators'] == 16
        assert c['generator_window'] == c['generator_capacity'] == 64
        assert c['capacity'] == PROFILE_SPECS[r['public_profile']]['capacity']
        assert c['bulk_bytes'] == PROFILE_SPECS[r['public_profile']]['bulk_bytes']
        assert r['wall_signals'] == exact_wall_signals(r)
        assert r['server']['tls_enabled'] == int(mode == 'tls-zstd')
        assert r['server']['compression'] == ('zstd' if mode == 'tls-zstd' else 'none')
        for cmd in [r['server_command'], *r['client_commands']]:
            assert cmd[0] == str(binary)
            assert cmd[cmd.index('--tls')+1] == ('1' if mode == 'tls-zstd' else '0')
            assert cmd[cmd.index('--compression')+1] == ('zstd' if mode == 'tls-zstd' else 'none')
        all_cases.append(dict(kind=kind, mode=mode, paired_trial=trial,
                              output=str(destination), result=r))
    print(f'benchmark {mode} {kind} paired_trial={trial}: clean={sum(clean_result(r) for r in cases)}/3', flush=True)

for mode in ('plain', 'tls-zstd'):
    for kind in ('baseline', 'candidate'):
        run(kind, mode, 'smoke')
for mode in ('plain', 'tls-zstd'):
    for trial, order in enumerate(manifest['paired_order'], 1):
        for kind in order:
            run(kind, mode, 'repeat', trial)
summary = []
for mode in ('plain', 'tls-zstd'):
    for kind in ('baseline', 'candidate'):
        for profile in PROFILE_SPECS:
            cases = [r['result'] for r in all_cases
                     if r['kind']==kind and r['mode']==mode
                     and r['result']['public_profile']==profile]
            assert len(cases)==3
            summary.append(dict(kind=kind, mode=mode,
                                summary=summarize(profile, cases)))
assert len(all_cases)==36
(out / 'comparison.json').write_text(json.dumps(dict(manifest=manifest, summaries=summary,
                                                   cases=all_cases, execution=events), indent=2)+'\n')
print('Completed all 36 fixed-input cases; no dirty sample omitted', flush=True)
