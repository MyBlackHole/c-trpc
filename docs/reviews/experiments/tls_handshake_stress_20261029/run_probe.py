"""驱动现有 bench_rpc 的有界 TLS 握手压力诊断；不是容量/总内存验收。"""
import errno
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import shutil
import socket
import ssl
import subprocess
import sys
import time

sys.dont_write_bytecode = True
root = Path(__file__).resolve().parents[2]
os.chdir(root)
sys.path.insert(0, str(root/'bench'))
from run_rpc_bench import metadata, read_ready, stop_owned, validate_server_exit
from run_rpc_capacity import validate_open_phase

seconds = int(sys.argv[1])
label = sys.argv[2]
assert 25 <= seconds <= 3600 and re.fullmatch(r'[a-z0-9-]+', label)
assert subprocess.check_output(['git','status','--porcelain'],text=True) == ''
head = subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()
out = root/'build/performance-20261029'/label
out.mkdir(exist_ok=False)
workspace_tmp = root/'build/test-tmp'
env = dict(os.environ,TMPDIR=str(workspace_tmp),TMP=str(workspace_tmp),TEMP=str(workspace_tmp),
           XMAKE_TMPDIR=str(workspace_tmp),PYTHONDONTWRITEBYTECODE='1')
binary = out/'bench_rpc'
shutil.copy2(root/'build/linux/x86_64/release/bench_rpc',binary)
identity = root/'build/performance-20261029/tls-zstd-hour-3600s'
ca, cert, key = [identity/name for name in ('ca.pem','server.pem','server.key')]
for path in (ca,cert,key): assert path.is_file()
common = ['--capacity','128','--bulk-bytes','65536','--small-bytes','4096',
          '--tls','1','--tls-ca',str(ca),'--tls-cert',str(cert),'--tls-key',str(key),
          '--compression','zstd']
server_command = [str(binary),'server',*common,'--slow-ms','0']
info = metadata(binary,label,load_mode='open',compression='zstd',tls=True)
info.update(type='tls_handshake_stress_metadata',source_commit=head,duration_s=seconds,
            binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
            harness_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            rpc_rate_rps=100,attack_rate_per_s=64,max_attack_sockets=64,
            server_max_peers=32,server_handshake_timeout_ms=10000,
            attack_kinds=['silent','partial_drip','clienthello_abort','plaintext'],
            tls_client_auth=False,server_command=server_command,
            scope='loopback long-lived server; observed RSS/PSS/fd/thread; not hard memory cap or capacity acceptance')
(out/'metadata.json').write_text(json.dumps(info,indent=2)+'\n')

ctx=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ctx.minimum_version=ssl.TLSVersion.TLSv1_3
ctx.maximum_version=ssl.TLSVersion.TLSv1_3
incoming,outgoing=ssl.MemoryBIO(),ssl.MemoryBIO()
hello_client=ctx.wrap_bio(incoming,outgoing,server_side=False,server_hostname='server.example.test')
try: hello_client.do_handshake()
except ssl.SSLWantReadError: pass
hello=outgoing.read()
assert 5<len(hello)<=65536 and hello[0]==0x16

children,logs,active,samples=[],[],{},[]
counts={kind:dict(attempted=0,connected=0,connect_errors=0,remote_closed=0,local_closed=0,bytes_sent=0,drip_writes=0,remote_age_s=[]) for kind in info['attack_kinds']}
selector=selectors.DefaultSelector()
peak_active=0
skipped=0
started=time.monotonic()

def sample(server,phase):
    proc=Path('/proc')/str(server.pid)
    status=dict(re.findall(r'^(\w+):\s*(\d+)',(proc/'status').read_text(),re.M))
    rollup=dict(re.findall(r'^(\w+):\s*(\d+)',(proc/'smaps_rollup').read_text(),re.M))
    row=dict(elapsed_s=round(time.monotonic()-started,3),phase=phase,
             server={k:int(status[k]) for k in ('VmRSS','VmHWM','RssAnon','VmData','Threads')},
             attack_active=len(active),attack_connected=sum(v['connected'] for v in counts.values()),
             attack_remote_closed=sum(v['remote_closed'] for v in counts.values()))
    row['server'].update({k:int(rollup[k]) for k in ('Pss','Private_Clean','Private_Dirty')})
    row['server']['fd_count']=len(list((proc/'fd').iterdir()))
    samples.append(row)
    with (out/'resources.jsonl').open('a') as log: log.write(json.dumps(row)+'\n')
    return row

def close_attack(sock,remote=False,connect_error=False):
    state=active.pop(sock)
    selector.unregister(sock)
    sock.close()
    c=counts[state['kind']]
    if connect_error:
        c['connect_errors']+=1
    else:
        c['remote_closed' if remote else 'local_closed']+=1
        if remote: c['remote_age_s'].append(time.monotonic()-state['born'])

try:
    server_err=(out/'server.stderr').open('w');logs.append(server_err)
    server=subprocess.Popen(server_command,env=env,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=server_err,text=True)
    children.append(server)
    ready=read_ready(server,'ready')
    assert ready['tls_enabled']==1 and ready['compression']=='zstd'
    client_command=[str(binary),'client',*common,'--port',str(ready['port']),
                    '--scenario','open','--requests',str(seconds*100),'--rate-rps','100',
                    '--window','64','--warmup','32','--timeout-ms','3000','--start-gate','1']
    info['client_command']=client_command
    (out/'metadata.json').write_text(json.dumps(info,indent=2)+'\n')
    client_err=(out/'client.stderr').open('w');logs.append(client_err)
    client=subprocess.Popen(client_command,env=env,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=client_err,text=True)
    children.append(client)
    client_ready=read_ready(client,'client_ready')
    for i in range(3):
        sample(server,'baseline')
        time.sleep(1)
    client.stdin.write('g');client.stdin.flush()
    begin=time.monotonic()
    next_open,next_sample,next_report=begin,begin,begin
    offered=0
    while time.monotonic()-begin<seconds:
        now=time.monotonic()
        assert server.poll() is None and client.poll() is None, 'owned process exited before attack interval'
        if now>=next_open:
            next_open+=1/64
            if len(active)>=64:
                skipped+=1
            else:
                kind=info['attack_kinds'][offered%4]
                offered+=1
                counts[kind]['attempted']+=1
                sock=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                sock.setblocking(False)
                code=sock.connect_ex(('127.0.0.1',ready['port']))
                state=dict(kind=kind,born=now,connected=False,pending=b'',next_drip=now+1)
                active[sock]=state
                selector.register(sock,selectors.EVENT_READ|selectors.EVENT_WRITE)
                if code not in (0,errno.EINPROGRESS,errno.EWOULDBLOCK): close_attack(sock,connect_error=True)
                peak_active=max(peak_active,len(active))
        for event,mask in selector.select(0.005):
            sock=event.fileobj
            if sock not in active: continue
            state=active[sock]
            c=counts[state['kind']]
            if not state['connected']:
                if sock.getsockopt(socket.SOL_SOCKET,socket.SO_ERROR):
                    close_attack(sock,connect_error=True);continue
                state['connected']=True
                c['connected']+=1
                state['pending']={'silent':b'','partial_drip':bytes.fromhex('1603030040'),'clienthello_abort':hello,'plaintext':b'GET / HTTP/1.0\r\n\r\n'}[state['kind']]
            if mask&selectors.EVENT_READ:
                try: received=sock.recv(65536)
                except BlockingIOError: received=None
                except (ConnectionResetError,BrokenPipeError): received=b''
                if received==b'': close_attack(sock,remote=True);continue
            if state['pending']:
                try: sent=sock.send(state['pending'])
                except BlockingIOError: sent=0
                except (ConnectionResetError,BrokenPipeError): close_attack(sock,remote=True);continue
                c['bytes_sent']+=sent
                state['pending']=state['pending'][sent:]
            selector.modify(sock,selectors.EVENT_READ|(selectors.EVENT_WRITE if state['pending'] else 0))
        now=time.monotonic()
        for sock,state in list(active.items()):
            if state['kind']=='clienthello_abort' and state['connected'] and not state['pending'] and now-state['born']>=0.05:
                close_attack(sock);continue
            if state['kind']=='partial_drip' and state['connected'] and now>=state['next_drip'] and not state['pending']:
                state['pending']=b'\0'
                state['next_drip']=now+1
                counts['partial_drip']['drip_writes']+=1
                selector.modify(sock,selectors.EVENT_READ|selectors.EVENT_WRITE)
        if now>=next_sample:
            next_sample+=1
            row=sample(server,'attack')
            if now>=next_report:
                print(json.dumps(row),flush=True);next_report+=60
        if now-begin>seconds+15: raise TimeoutError('attack guard')
    for sock in list(active): close_attack(sock)
    stdout,_=client.communicate(timeout=30)
    (out/'client.jsonl').write_text(stdout)
    assert client.returncode==0
    rows=[json.loads(line) for line in stdout.splitlines() if line]
    assert len(rows)==1
    phase=rows[0]
    validate_open_phase(phase,seconds*100,100,64)
    assert phase['ok']==seconds*100, phase
    for i in range(12):
        assert server.poll() is None
        sample(server,'post_attack_idle')
        time.sleep(1)
    recovery_command=[str(binary),'client',*common,'--port',str(ready['port']),
                      '--scenario','small','--requests','64','--window','8','--warmup','0','--timeout-ms','3000']
    recovery=subprocess.run(recovery_command,env=env,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=30)
    (out/'recovery.jsonl').write_text(recovery.stdout)
    (out/'recovery.stderr').write_text(recovery.stderr)
    assert recovery.returncode==0
    recovery_row=json.loads(recovery.stdout)
    assert recovery_row['all']['ok']==64 and recovery_row['all']['completed']==64
    sample(server,'post_recovery')
    stdout,_=server.communicate(timeout=30)
    (out/'server.jsonl').write_text(json.dumps(ready)+'\n'+stdout)
    assert server.returncode==0
    server_exit=json.loads(stdout)
    validate_server_exit(server_exit)
    assert server_exit['rpc_calls_started']==seconds*100+32+64
    for c in counts.values():
        assert c['attempted']==c['connect_errors']+c['local_closed']+c['remote_closed']
    for kind in ('silent','partial_drip'):
        assert any(age>=5 for age in counts[kind]['remote_age_s']), (kind,counts[kind])
    summary=dict(status='diagnostic_completed',source_commit=head,duration_s=seconds,healthy_rpc_ok=phase['ok'],
                 recovery_rpc_ok=64,attack_sockets_peak=peak_active,attack_offers_skipped=skipped,
                 attack_counts=counts,client=phase,server_exit=server_exit,
                 resource_samples=len(samples),server_resource_ranges={k:[min(r['server'][k] for r in samples),max(r['server'][k] for r in samples)] for k in samples[0]['server']},
                 limitations=['not_total_tls_hard_cap','not_allocator_live_ledger','not_capacity_acceptance','not_mtls','not_per_socket_kernel_residency'])
    (out/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
    print(json.dumps(dict(event='stress_complete',duration_s=seconds,healthy_rpc_ok=phase['ok'],recovery_rpc_ok=64,
                         attack_counts={k:{n:v for n,v in c.items() if n!='remote_age_s'} for k,c in counts.items()},
                         samples=len(samples),server_resource_ranges=summary['server_resource_ranges'])),flush=True)
finally:
    for sock in list(active): close_attack(sock)
    selector.close()
    for child in reversed(children):
        stop_owned(child)
        for pipe in (child.stdin,child.stdout):
            if pipe is not None: pipe.close()
    for log in logs: log.close()
