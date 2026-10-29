"""只管理工作区内的可丢弃编码探针；不修改主工程构建图。"""
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import statistics
import subprocess
import time

here = Path(__file__).resolve().parent
root = here.parents[1]
os.chdir(here)
temp = root / 'build/test-tmp'
env = dict(os.environ, TMPDIR=str(temp), TMP=str(temp), TEMP=str(temp),
           XMAKE_TMPDIR=str(temp), PYTHONDONTWRITEBYTECODE='1',
           ASAN_OPTIONS='detect_leaks=1:halt_on_error=1',
           UBSAN_OPTIONS='halt_on_error=1:print_stacktrace=1')

def run(label, args, timeout=120, affinity=False):
    path = here / (label + '.log')
    start = time.monotonic()
    with path.open('w') as log:
        result = subprocess.run(args, env=env, stdout=log, stderr=subprocess.STDOUT,
                                timeout=timeout,
                                preexec_fn=(lambda: os.sched_setaffinity(0, {0})) if affinity else None)
    print(json.dumps(dict(event='phase_complete', phase=label,
                          exit_code=result.returncode,
                          elapsed_s=round(time.monotonic()-start, 3))), flush=True)
    if result.returncode:
        print(path.read_text()[-4000:], flush=True)
        raise RuntimeError(label)
    return path

for label, mode, compiler in [('gcc-release', 'release', 'gcc'),
                               ('clang-release', 'release', 'clang'),
                               ('gcc-asan', 'asan', 'gcc'),
                               ('gcc-final', 'release', 'gcc')]:
    run(label+'-configure', ['xmake', 'f', '-P', '.', '-c', '-y', '-m', mode,
                             '--toolchain='+compiler])
    log = run(label+'-check', ['xmake', 'test', '-P', '.', '-v', '-j1'])
    assert 'unknown_label_edit_roundtrip' in log.read_text()
    if label == 'gcc-release':
        (here/'frozen').mkdir(exist_ok=True)
        shutil.copyfile(here/'build/linux/x86_64/release/codec_spike',
                        here/'frozen/gcc-release')

binary = here/'build/linux/x86_64/release/codec_spike'
digest = lambda p: hashlib.sha256(p.read_bytes()).hexdigest()
assert digest(binary) == digest(here/'frozen/gcc-release')
metadata = dict(source_commit=subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
                platform=platform.platform(), compiler=subprocess.check_output(['gcc', '--version'], text=True).splitlines()[0],
                clang=subprocess.check_output(['clang', '--version'], text=True).splitlines()[0],
                xmake=subprocess.check_output(['xmake', '--version'], text=True).splitlines()[0],
                cpu_model=next(line.split(':', 1)[1].strip() for line in Path('/proc/cpuinfo').read_text().splitlines() if line.startswith('model name')),
                binary_sha256=digest(binary), affinity=[0], trials=9,
                warmup_trials=[0, 1], source_sha256={p.name:digest(p) for p in
                    (here/'codec.c', here/'codec.h', here/'measure.c', here/'xmake.lua', Path(__file__).resolve())},
                scope=['disposable_candidate', 'u64_bytes_optional_ascii_label',
                       'shared_host', 'hot_buffers', 'batch_cpu_means',
                       'not_rpc_tail_latency', 'not_upstream_implementation',
                       'no_arrays', 'no_nested_structures', 'no_full_utf8',
                       'no_rust', 'fixed_width_offset_edit_only'])
(here/'metadata.json').write_text(json.dumps(metadata, ensure_ascii=False, indent=2)+'\n')
log = run('gcc-measure', ['xmake', 'run', '-P', '.', 'codec_spike'], affinity=True)
rows = [json.loads(s) for s in log.read_text().splitlines() if s.startswith('{')]
assert len(rows) == 576
assert {r['trial'] for r in rows} == set(range(9))
assert digest(binary) == metadata['binary_sha256']
(here/'results.jsonl').write_text(''.join(json.dumps(r, sort_keys=True)+'\n' for r in rows))
summary = []
for version in (1, 2):
    for size in (0, 1, 4096, 65536):
        for phase in ('encode', 'verify', 'consume_all', 'copy_edit'):
            item = dict(version=version, data_bytes=size, phase=phase)
            for f in ('compact_candidate', 'block_candidate'):
                selected = [r for r in rows if (r['version'], r['data_bytes'], r['phase'], r['format']) == (version, size, phase, f) and r['trial'] >= 2]
                assert len(selected) == 7
                item[f] = dict(body_bytes=selected[0]['body_bytes'],
                               cpu_ns_per_op_median=round(statistics.median(r['cpu_ns_per_op'] for r in selected), 3),
                               cpu_ns_per_op_min=round(min(r['cpu_ns_per_op'] for r in selected), 3),
                               cpu_ns_per_op_max=round(max(r['cpu_ns_per_op'] for r in selected), 3))
            summary.append(item)
(here/'summary.json').write_text(json.dumps(summary, indent=2)+'\n')
print(json.dumps(dict(event='probe_complete', checks_passed=4,
                      measurement_rows=576, measured_trials=7)), flush=True)
