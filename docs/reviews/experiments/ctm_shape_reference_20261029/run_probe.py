"""经独立 Xmake 入口运行参考模型与六个负对照，只向工作区 build/ 写入。"""
import hashlib
import json
import os
from pathlib import Path
import subprocess

here = Path(__file__).resolve().parent
root = Path(subprocess.check_output(["git", "rev-parse", "--show-toplevel"], cwd=here, text=True).strip())
if here.parent != root / "build":
    raise RuntimeError("WORKSPACE_BUILD_DIRECTORY_REQUIRED")
temp = root / "build/test-tmp"
temp.mkdir(parents=True, exist_ok=True)
env = dict(os.environ, TMPDIR=str(temp), TMP=str(temp), TEMP=str(temp),
           XMAKE_TMPDIR=str(temp), PYTHONDONTWRITEBYTECODE="1")
env.pop("CTM_SHAPE_MUTATION", None)
env.pop("CTM_SHAPE_ONLY", None)


def run(label, mutation=None, fmt=None):
    settings = dict(env)
    if mutation:
        settings.update(CTM_SHAPE_MUTATION=mutation, CTM_SHAPE_ONLY=fmt)
    log = here / (label + ".log")
    with log.open("w") as output:
        result = subprocess.run(["xmake", "check_shapes", "-P", "."], cwd=here,
                                env=settings, stdout=output, stderr=subprocess.STDOUT,
                                timeout=60)
    if not mutation:
        if result.returncode:
            raise RuntimeError((label, result.returncode, str(log)))
    else:
        marker = {"drop_root": "ROOT_UNKNOWN_LOSS", "drop_nested": "NESTED_UNKNOWN_LOSS",
                  "reset_budget": "EXPECTED_REJECTION"}[mutation]
        contents = log.read_text()
        events = [json.loads(line) for line in contents.splitlines() if line.startswith("{")]
        expected = dict(event="negative_control_start", mutation=mutation, formats=[fmt])
        if result.returncode == 0 or marker not in contents or expected not in events:
            raise RuntimeError(("NEGATIVE_CONTROL_NOT_CAUGHT", label, str(log)))
    return dict(label=label, mutation=mutation, format=fmt, exit_code=result.returncode,
                log_sha256=hashlib.sha256(log.read_bytes()).hexdigest())


phases = [run("baseline-initial")]
initial = (here / "results.json").read_bytes()
for mutation in ("drop_root", "drop_nested", "reset_budget"):
    for fmt in ("compact", "block"):
        phases.append(run("control-" + mutation + "-" + fmt, mutation, fmt))
phases.append(run("baseline-final"))
if initial != (here / "results.json").read_bytes():
    raise RuntimeError("NONDETERMINISTIC_MODEL_RESULT")
controls = dict(source_commit=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
                phases=phases, baseline_passes=2, controls_caught=6,
                result_sha256=hashlib.sha256(initial).hexdigest())
(here / "controls.json").write_text(json.dumps(controls, indent=2) + "\n")
print(json.dumps(dict(event="shape_probe_complete", baseline_passes=2,
                      controls_caught=6, result_sha256=controls["result_sha256"])))
