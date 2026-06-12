#!/usr/bin/env python3
"""
SSH 设备监控 - 双击运行
运行后自动弹出浏览器配置页面，无需手动输入任何网址。
"""
import http.server
import json
import os
import re
import sys
import time
import threading
import queue
import urllib.parse
import socketserver

import paramiko

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, 'data')
PORT = 8080

os.makedirs(DATA, exist_ok=True)

# ======================= 全局状态 =======================

class S:
    running = False
    latest = {}
    history = {}
    logs = []
    start_time = None
    cycles = 0
    stop = threading.Event()
    sse_clients = []
    sse_lock = threading.Lock()

def push_update(rec):
    """采集到新数据后推送给所有 SSE 客户端"""
    payload = json.dumps({'command': rec['command'], 'time': rec['time'],
                          'parsed': rec['parsed'], 'raw': rec['raw']}, ensure_ascii=False)
    with S.sse_lock:
        dead = []
        for q in S.sse_clients:
            try: q.put(payload)
            except: dead.append(q)
        for q in dead: S.sse_clients.remove(q)

def log(msg):
    line = f'[{time.strftime("%H:%M:%S")}] {msg}'
    S.logs.append(line)
    if len(S.logs) > 200: S.logs = S.logs[-200:]
    print(line, flush=True)

# ======================= SSH 采集 =======================

def parse_table(text, parse_fields=None, row_filter=None):
    lines = text.split('\n')
    parsed, headers, found = [], [], False
    for line in lines:
        s = line.strip()
        if re.match(r'^\s*[A-Z][A-Z_0-9]+\s+[A-Z]', s) and not found:
            headers = [h.strip() for h in re.split(r'\s{2,}', s) if h.strip()]
            if len(headers) >= 2: found = True; continue
        if found:
            is_data = bool(re.match(r'^\s*\d', s))
            if is_data:
                parts = [p.strip() for p in re.split(r'\s{2,}', s) if p.strip()]
                if len(parts) < 2: parts = s.split()
                if len(parts) < 2:
                    log(f'[parse] 数据行分割失败: {repr(s)[:80]}')
                    continue
                row = {}
                if headers and len(parts) >= len(headers):
                    for i, h in enumerate(headers):
                        if i < len(parts): row[h] = parts[i]
                else:
                    row['COL0'], row['COL1'] = parts[0], parts[-1]
                if row_filter:
                    row_upper = {k.upper(): v for k, v in row.items()}
                    if not all(str(row_upper.get(k.upper(), '')) == str(v) for k, v in row_filter.items()):
                        log(f'[parse] 行过滤排除: {row}')
                        continue
                if parse_fields:
                    row = {k: v for k, v in row.items()
                           if k.upper() in [f.upper().strip() for f in parse_fields]}
                if row: parsed.append(row)
    if not found:
        log(f'[parse] 未找到表头, 总行数={len(lines)}, 首行={repr(lines[0])[:80]}')
    elif not parsed:
        log(f'[parse] 表头={headers}, 0条结果, parse_fields={parse_fields}, row_filter={row_filter}')
    return parsed


def collect(config):
    ssh = ch = None
    t0 = time.time()
    cmds = [c for c in config.get('commands', []) if c.get('enabled', True)]
    for c in cmds:
        if c['name'] not in S.history: S.history[c['name']] = []

    try:
        cfg = config['ssh']
        log(f'SSH 连接 {cfg["host"]}:{cfg["port"]} ...')
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(cfg['host'], cfg['port'], cfg['username'], cfg['password'],
                     timeout=20, allow_agent=False, look_for_keys=False)
        log('SSH 连接成功')

        ch = ssh.invoke_shell(term='vt100', width=200, height=50)
        time.sleep(1)
        for cmd in cfg.get('shell_init_commands', []):
            ch.send(cmd + '\n'); time.sleep(0.5)
        time.sleep(0.5)
        while ch.recv_ready():
            try: ch.recv(4096)
            except: break

        t = config['telnet']
        ch.send(t['command'] + '\n'); time.sleep(2)
        ch.send(t['login_command'] + '\n'); time.sleep(3)
        while ch.recv_ready():
            try: ch.recv(4096)
            except: break
        log('Telnet 登录成功')

        interval = config['monitor'].get('interval_seconds', 1)
        total = config['monitor'].get('total_seconds', 60)
        cycle = 0
        S.start_time = time.time()

        while not S.stop.is_set():
            elapsed = time.time() - S.start_time
            if total > 0 and elapsed >= total: log(f'已达时长 {total}s'); break

            for cmd_cfg in cmds:
                if S.stop.is_set(): break
                cmd_start = time.time()
                try:
                    ch.send(cmd_cfg['command'] + '\n')
                    data = b''
                    dl = time.time() + 15
                    while time.time() < dl:
                        if ch.recv_ready():
                            try: data += ch.recv(4096)
                            except: break
                        else: time.sleep(0.1)
                        txt = data.decode('utf-8', 'replace')
                        if any(m in txt for m in ['OK!', 'ERROR', '\n# ', '\n> ']): break
                    resp = data.decode('utf-8', 'replace')
                except Exception as e:
                    log(f'命令异常: {e}'); break

                parsed = parse_table(resp, cmd_cfg.get('parse_fields'), cmd_cfg.get('row_filter'))
                m = re.search(r'\[(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\]', resp)
                device_time = m.group(1) if m else time.strftime('%Y-%m-%d %H:%M:%S')
                rec = {'time': device_time,
                       'command': cmd_cfg['name'], 'parsed': parsed, 'raw': resp}
                name = cmd_cfg['name']
                S.latest[name] = rec
                S.history[name].append(rec)
                if len(S.history[name]) > 200000: S.history[name] = S.history[name][-200000:]
                if len(S.history[name]) % 1000 == 0:
                    try:
                        fp = os.path.join(DATA, f'{name}.json')
                        with open(fp, 'w', encoding='utf-8') as f:
                            json.dump(S.history[name], f, ensure_ascii=False)
                    except: pass
                push_update(rec)
                cycle += 1
                S.cycles = cycle
                elapsed = time.time() - S.start_time
                log(f'#{cycle} {name}: {len(parsed)} 条 ({elapsed:.0f}s/{total}s)')

                remain = interval - (time.time() - cmd_start)
                while remain > 0 and not S.stop.is_set():
                    try: ch.send('\n'); time.sleep(0.15)
                    except: pass
                    while ch.recv_ready():
                        try: ch.recv(4096)
                        except: break
                    remain = interval - (time.time() - cmd_start)

    except Exception as e:
        log(f'错误: {e}')
        import traceback; log(traceback.format_exc())
    finally:
        try: ch.close()
        except: pass
        try: ssh.close()
        except: pass
        S.running = False
        log(f'采集结束 (运行 {time.time()-t0:.0f}s)')
        for name, records in S.history.items():
            try:
                with open(os.path.join(DATA, f'{name}.json'), 'w', encoding='utf-8') as f:
                    json.dump(records, f, ensure_ascii=False, indent=2)
            except: pass


# ======================= HTTP 服务 =======================

class H(http.server.BaseHTTPRequestHandler):
    def _json(self, d, s=200):
        b = json.dumps(d, ensure_ascii=False).encode('utf-8')
        self.send_response(s)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Content-Length', str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def do_GET(self):
        p = urllib.parse.urlparse(self.path)
        if p.path == '/' or p.path == '/index.html':
            body = DASHBOARD.encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif p.path == '/api/status':
            e = int(time.time() - S.start_time) if S.start_time and S.running else 0
            self._json({'running': S.running, 'ever_started': S.start_time is not None,
                         'elapsed': e, 'cycles': S.cycles})
        elif p.path == '/api/data':
            self._json(S.latest)
        elif p.path == '/api/commands':
            cmds = [f.replace('.json', '') for f in os.listdir(DATA) if f.endswith('.json')]
            self._json(cmds)
        elif p.path == '/api/history':
            q = urllib.parse.parse_qs(p.query); cmd = q.get('command', [''])[0]
            limit = int(q.get('limit', ['0'])[0]) or 0
            data = S.history.get(cmd, None)
            if data is not None:
                self._json(data[-limit:] if limit and len(data) > limit else data)
            else:
                fp = os.path.join(DATA, f'{cmd}.json')
                data = json.load(open(fp, encoding='utf-8')) if os.path.exists(fp) else []
                self._json(data[-limit:] if limit and len(data) > limit else data)
        elif p.path == '/api/logs':
            self._json({'logs': S.logs[-100:]})
        elif p.path == '/api/stream':
            self.send_response(200)
            self.send_header('Content-Type', 'text/event-stream')
            self.send_header('Cache-Control', 'no-cache')
            self.send_header('Connection', 'keep-alive')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            q = queue.Queue()
            with S.sse_lock:
                S.sse_clients.append(q)
            try:
                init_payload = json.dumps(S.latest, ensure_ascii=False)
                self.wfile.write(f'event: init\ndata: {init_payload}\n\n'.encode())
                self.wfile.flush()
                while not S.stop.is_set():
                    try:
                        data = q.get(timeout=15)
                        self.wfile.write(f'data: {data}\n\n'.encode())
                        self.wfile.flush()
                    except queue.Empty:
                        self.wfile.write(b':keepalive\n\n')
                        self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError, OSError):
                pass
            finally:
                with S.sse_lock:
                    if q in S.sse_clients:
                        S.sse_clients.remove(q)
        else:
            self._json({'error': 'not found'}, 404)

    def do_POST(self):
        p = urllib.parse.urlparse(self.path)
        if p.path == '/api/start':
            try: config = json.loads(self.rfile.read(int(self.headers.get('Content-Length', 0))))
            except: self._json({'error': '无效 JSON'}, 400); return
            if S.running: self._json({'error': '已在运行中'}, 409); return
            S.latest = {}; S.history = {}; S.logs = []; S.cycles = 0
            S.stop.clear(); S.running = True; S.start_time = None
            threading.Thread(target=collect, args=(config,), daemon=True).start()
            self._json({'message': '采集已启动'})
        elif p.path == '/api/stop':
            S.stop.set(); S.running = False
            self._json({'message': '已停止'})
        else:
            self._json({'error': 'not found'}, 404)

    def log_message(self, *a): pass


# ======================= HTML 页面 =======================

DASHBOARD = '''<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>SSH 设备监控</title>
<script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
<style>
:root{--bg:#0f172a;--card:#1e293b;--border:#334155;--text:#e2e8f0;--muted:#94a3b8;--subtle:#64748b;--green:#22c55e;--blue:#3b82f6;--red:#ef4444}
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:var(--bg);color:var(--text);min-height:100vh}
.header{background:linear-gradient(135deg,#1e293b,#0f172a);border-bottom:1px solid var(--border);padding:14px 28px;display:flex;align-items:center;justify-content:space-between}
.header h1{font-size:20px}
.status{display:flex;align-items:center;gap:8px}
.help-btn{background:var(--border);color:var(--text);border:none;border-radius:6px;padding:5px 14px;font-size:13px;cursor:pointer;transition:background .2s}
.help-btn:hover{background:var(--muted)}
.modal-overlay{display:none;position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,.6);z-index:999;justify-content:center;align-items:flex-start;padding:60px 20px}
.modal-overlay.show{display:flex}
.modal{background:var(--card);border:1px solid var(--border);border-radius:12px;max-width:760px;width:100%;max-height:80vh;overflow-y:auto;padding:28px 32px;position:relative}
.modal h2{font-size:16px;color:var(--text);margin-bottom:18px}
.modal h4{font-size:13px;color:var(--muted);margin:16px 0 6px}
.modal p,.modal li{font-size:13px;line-height:1.7;color:var(--muted)}
.modal code{background:#0f172a;padding:2px 6px;border-radius:4px;font-size:12px;color:var(--green)}
.modal pre{background:#0f172a;padding:12px;border-radius:8px;font-size:12px;color:var(--text);overflow-x:auto;margin:8px 0}
.modal-close{position:absolute;top:14px;right:18px;background:none;border:none;color:var(--subtle);font-size:22px;cursor:pointer}
.modal-close:hover{color:var(--text)}
.dot{width:10px;height:10px;border-radius:50%;background:var(--red)}
.dot.on{background:var(--green);box-shadow:0 0 8px rgba(34,197,94,0.5)}
.container{max-width:1400px;margin:0 auto;padding:20px 28px}
.card{background:var(--card);border-radius:12px;border:1px solid var(--border);padding:20px;margin-bottom:16px}
.card h3{font-size:15px;color:var(--muted);margin-bottom:14px;text-transform:uppercase;letter-spacing:1px}
.row{display:flex;gap:12px;flex-wrap:wrap;align-items:flex-end}
.field{display:flex;flex-direction:column;gap:4px}
.field label{font-size:12px;color:var(--muted)}
.field input{background:#0f172a;color:var(--text);border:1px solid var(--border);border-radius:6px;padding:8px 12px;font-size:13px;width:120px}
.field input.wide{width:200px}.field input.wider{width:300px}
.btn{padding:8px 24px;border:none;border-radius:8px;cursor:pointer;font-size:14px;font-weight:600}
.btn-start{background:var(--green);color:#fff}.btn-start:hover{background:#16a34a}
.btn-stop{background:var(--red);color:#fff}.btn-stop:hover{background:#dc2626}
.btn:disabled{opacity:0.4;cursor:not-allowed}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:16px}
.chart-box{width:100%;height:300px}
table{width:100%;border-collapse:collapse;font-size:13px}
th{text-align:left;padding:8px 12px;background:var(--border);color:var(--muted);font-weight:500;font-size:12px}
td{padding:8px 12px;border-bottom:1px solid var(--border)}
tr:hover td{background:#293548}
.log-box{font-family:'Consolas','Courier New',monospace;font-size:12px;max-height:200px;overflow-y:auto;background:#0a0f1a;border-radius:8px;padding:12px;line-height:1.6;word-break:break-all}
.badge{display:inline-block;padding:2px 8px;border-radius:10px;font-size:11px;font-weight:600}
.badge.on,.badge.enable{background:rgba(34,197,94,0.2);color:var(--green)}
.badge.off,.badge.disable{background:rgba(239,68,68,0.2);color:var(--red)}
@media(max-width:900px){.grid{grid-template-columns:1fr}}
</style>
</head>
<body>
<div class="header">
<h1>SSH 设备远程监控</h1>
<div class="status"><button class="help-btn" onclick="showHelp()">使用说明</button><span id="dot" class="dot"></span><span id="st">未启动</span></div>
</div>
<div class="container">
<div class="card"><h3>采集配置</h3>
<div class="row" style="margin-bottom:10px">
<div class="field"><label>设备 IP</label><input id="ip" value="197.6.7.23"></div>
<div class="field"><label>端口</label><input id="port" value="22" style="width:65px"></div>
<div class="field"><label>SSH 密码</label><input id="pw" value="andisat" type="password" style="width:90px"></div>
<div class="field"><label>间隔(秒)</label><input id="iv" value="1" style="width:65px"></div>
<div class="field"><label>时长(秒,0=无限)</label><input id="dur" value="60" style="width:75px"></div>
</div>
<div class="row" style="margin-bottom:10px">
<div class="field"><label>命令1</label><input id="c1" value="get-if-rx-curesn0:@bid=3&pid=1&chid=1" class="wider"></div>
<div class="field"><label>解析字段1</label><input id="f1" value="ESN0" class="wide"></div>
<div class="field"><label>行过滤1</label><input id="r1" value="" class="wide" placeholder="如 BID=3 或留空"></div>
</div>
<div class="row" style="margin-bottom:14px">
<div class="field"><label>命令2(可选)</label><input id="c2" value="" class="wider" placeholder="留空不执行"></div>
<div class="field"><label>解析字段2</label><input id="f2" value="" class="wide"></div>
<div class="field"><label>行过滤2</label><input id="r2" value="" class="wide"></div>
</div>
<div class="row">
<button class="btn btn-start" id="btnS" onclick="start()">启动采集</button>
<button class="btn btn-stop" id="btnP" onclick="stop()" disabled>停止采集</button>
</div>
</div>
<div class="grid">
<div class="card"><h3>最新数据</h3>
<div id="dataTbl"><p style="color:var(--subtle);font-size:13px">等待数据...</p></div>
<div id="dataTime" style="font-size:11px;color:var(--subtle);margin-top:8px"></div>
</div>
<div class="card"><h3>命令原始返回</h3>
<pre id="rawBox" style="font-family:'Consolas','Courier New',monospace;font-size:11px;color:var(--muted);max-height:200px;overflow-y:auto;white-space:pre-wrap;word-break:break-all;line-height:1.5">等待数据...</pre></div>
</div>
<div class="card"><h3 style="display:flex;align-items:center;gap:10px">实时图表 <button class="btn btn-start" style="font-size:11px;padding:4px 12px" onclick="renderFullChart()">查看全量</button></h3>
<div id="chartBox" class="chart-box"></div></div>
<div class="card"><h3>数据分析</h3>
<div class="row" style="margin-bottom:10px">
<button class="btn btn-start" id="btnAnalyze" onclick="analyze()">执行分析</button>
</div>
<div id="analysisBox" style="max-height:500px;overflow-y:auto;user-select:text">
<p style="color:var(--subtle);font-size:13px">点击"执行分析"按钮开始</p>
</div></div>
<div class="card"><h3 style="display:flex;align-items:center;gap:10px">命令执行记录 <button class="btn btn-start" style="font-size:11px;padding:4px 12px" onclick="copyCmdHistory()">复制</button></h3>
<div id="cmdHistoryBox" style="max-height:400px;overflow-y:auto;user-select:text">
<p style="color:var(--subtle);font-size:13px">等待启动...</p>
</div></div>
<div class="card"><h3>运行日志</h3>
<div class="log-box" id="logBox">等待启动...</div></div>
</div>
<script>
var chart=null,timer=null,data={},sel=null,cmdHistory=[];
function $(id){return document.getElementById(id)}
function pf(s){if(!s||!s.includes('='))return null;var p=s.split('=');var r={};r[p[0].trim()]=p[1].trim();return r}
function pp(s){if(!s)return[];return s.split(',').map(function(v){return v.trim()}).filter(Boolean)}
function start(){
var cmds=[];
if($('c1').value.trim())cmds.push({name:$('c1').value.trim().replace(/[:@&=]/g,'-'),command:$('c1').value.trim(),enabled:true,parse_fields:pp($('f1').value),row_filter:pf($('r1').value)||{}});
if($('c2').value.trim())cmds.push({name:$('c2').value.trim().replace(/[:@&=]/g,'-'),command:$('c2').value.trim(),enabled:true,parse_fields:pp($('f2').value),row_filter:pf($('r2').value)||{}});
if(!cmds.length){alert('请填写命令');return}
var body={ssh:{host:$('ip').value.trim(),port:parseInt($('port').value)||22,username:'root',password:$('pw').value,shell_init_commands:['export TMOUT=0']},telnet:{command:'telnet 0 2323',login_command:'login:root,Changeme_123'},commands:cmds,monitor:{interval_seconds:parseInt($('iv').value)||1,total_seconds:parseInt($('dur').value)||60}};
$('btnS').disabled=true;$('btnS').textContent='连接中...';
fetch('http://localhost:8080/api/start',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)})
.then(function(r){return r.json()}).then(function(j){
if(j.error){alert(j.error);$('btnS').disabled=false;$('btnS').textContent='启动采集';return}
$('logBox').innerHTML='[系统] '+j.message+'<br>';$('btnS').textContent='启动采集';cmdHistory=[];pollLoop()
}).catch(function(e){alert('失败: '+e);$('btnS').disabled=false;$('btnS').textContent='启动采集'})
}
function stop(){$('btnP').disabled=true;fetch('http://localhost:8080/api/stop',{method:'POST'});$('logBox').innerHTML+='[系统] 已停止<br>'}
function pollLoop(){
if(window._sse)window._sse.close();
window._sse=new EventSource('http://localhost:8080/api/stream');
window._sse.addEventListener('init',function(e){try{
var dr=JSON.parse(e.data);
if(!Object.keys(dr).length){
fetch('http://localhost:8080/api/commands').then(function(r){return r.json()}).then(function(cmds){
if(!cmds||!cmds.length)return;
cmds.forEach(function(cmd){
fetch('http://localhost:8080/api/history?command='+cmd+'&limit=2000').then(function(r){return r.json()}).then(function(h){
if(!h||!h.length)return;
data[cmd]=h[h.length-1];if(!sel)sel=cmd;
renderTbl(data);
h.forEach(function(rec){cmdHistory.push(rec)});
cmdHistory.sort(function(a,b){return a.time<b.time?-1:a.time>b.time?1:0});
if(cmdHistory.length>2000)cmdHistory=cmdHistory.slice(-2000);
renderCmdHistory()
})
})
})
}else{
data=dr;if(!sel||!dr[sel])sel=Object.keys(dr)[0];renderTbl(dr);addChartPoint(dr);
Object.keys(dr).forEach(function(cmd){
fetch('http://localhost:8080/api/history?command='+cmd+'&limit=2000').then(function(r){return r.json()}).then(function(h){
var existed=cmdHistory.filter(function(x){return x.command===cmd}).length;
(h||[]).slice(existed).forEach(function(rec){cmdHistory.push(rec)});
cmdHistory.sort(function(a,b){return a.time<b.time?-1:a.time>b.time?1:0});
if(cmdHistory.length>2000)cmdHistory=cmdHistory.slice(-2000);
renderCmdHistory()
})
})
}
}catch(er){}});
window._sse.addEventListener('message',function(e){try{
var rec=JSON.parse(e.data);data[rec.command]=rec;
sel=rec.command;renderTbl(data);addChartPoint(data);
cmdHistory.push(rec);if(cmdHistory.length>2000)cmdHistory=cmdHistory.slice(-2000);renderCmdHistory()
}catch(er){}});
if(timer)clearInterval(timer);timer=setInterval(poll,2000);poll()
}
function poll(){
fetch('http://localhost:8080/api/status').then(function(r){return r.json()}).then(function(sr){
var d=$('dot'),t=$('st');
if(sr.running){d.className='dot on';t.textContent='运行中'+(sr.elapsed?'('+sr.elapsed+'s)':'');$('btnS').disabled=true;$('btnP').disabled=false}
else{d.className='dot';t.textContent=sr.ever_started?'已停止':'未启动';if(!sr.running&&sr.ever_started){$('btnS').disabled=false;$('btnP').disabled=true;stopPoll()}}
});
fetch('http://localhost:8080/api/logs').then(function(r){return r.json()}).then(function(lr){
if(lr.logs&&lr.logs.length){$('logBox').innerHTML=lr.logs.slice(-50).map(function(l){return l.replace(/</g,'&lt;')}).join('<br>');$('logBox').scrollTop=$('logBox').scrollHeight}
})
}
function renderTbl(d){
var r=d[sel];if(!r){$('dataTbl').innerHTML='<p style="color:var(--subtle);font-size:13px">等待数据...</p>';return}
if(r.raw){$('rawBox').textContent=r.raw}else{$('rawBox').textContent='(无原始数据)'}
if(!r.parsed||!r.parsed.length){$('dataTbl').innerHTML='<p style="color:var(--subtle);font-size:13px">暂无解析数据 (0条)</p>';$('dataTime').textContent='时间: '+(r.time||'');return}
$('dataTime').textContent='时间: '+(r.time||'');var cols=Object.keys(r.parsed[0]),h='<table><thead><tr>';
cols.forEach(function(c){h+='<th>'+c+'</th>'});h+='</tr></thead><tbody>';
r.parsed.forEach(function(row){h+='<tr>';cols.forEach(function(c){var v=String(row[c]||''),lo=v.toLowerCase();h+=['enable','disable','on','off'].indexOf(lo)>=0?'<td><span class="badge '+lo+'">'+v+'</span></td>':'<td>'+v+'</td>'});h+='</tr>'});
h+='</tbody></table>';$('dataTbl').innerHTML=h
}
function analyze(){
var cmds=Object.keys(data);
if(!cmds.length){$('analysisBox').innerHTML='<p style="color:var(--subtle)">暂无数据，请先启动采集</p>';return}
$('analysisBox').innerHTML='<p style="color:var(--subtle)">分析中...</p>';
var pending=cmds.length;
var allGroups={};
cmds.forEach(function(cmd){
fetch('http://localhost:8080/api/history?command='+cmd+'&limit=200000').then(function(r){return r.json()}).then(function(h){
allGroups[cmd]=(h||[]).sort(function(a,b){return a.time<b.time?-1:a.time>b.time?1:0});
pending--;if(pending===0)doAnalyze(allGroups)
}).catch(function(){pending--;if(pending===0)doAnalyze(allGroups)})
})
}
function doAnalyze(groups){
var gkeys=Object.keys(groups).filter(function(k){return groups[k]&&groups[k].length});
if(!gkeys.length){$('analysisBox').innerHTML='<p style="color:var(--subtle)">暂无记录可分析</p>';return}
var html='';
gkeys.forEach(function(cmd){
var recs=groups[cmd];
if(!recs.length)return;
var sample=recs[0].parsed;
if(!sample||!sample.length)return;
var cols=Object.keys(sample[0]);
cols.forEach(function(col){
var vals=recs.map(function(r){return r.parsed&&r.parsed.length?r.parsed[0][col]:undefined}).filter(function(v){return v!==undefined});
if(!vals.length)return;
var numeric=vals.every(function(v){return isNum(v)});
html+='<div style="margin-bottom:12px;padding:10px;background:#0f172a;border-radius:8px;border:1px solid var(--border)">';
html+='<div style="font-weight:600;color:var(--muted);margin-bottom:6px">'+cmd+' / '+col+' ('+vals.length+'条, '+(numeric?'数值':'文本')+')</div>';
if(numeric){
var nums=vals.map(Number);
var sorted=nums.slice().sort(function(a,b){return a-b});
var n=nums.length;
var min=sorted[0],max=sorted[n-1];
var sum=nums.reduce(function(a,b){return a+b},0);
var avg=sum/n;
var md=n%2===1?sorted[Math.floor(n/2)]:(sorted[n/2-1]+sorted[n/2])/2;
var vr=nums.reduce(function(s,v){return s+(v-avg)*(v-avg)},0)/n;
var sd=Math.sqrt(vr);
var first=nums[0],last=nums[n-1];
var tr=last>first*1.001?'<span style="color:var(--green)">↑ 上升</span>':last<first*0.999?'<span style="color:var(--red)">↓ 下降</span>':'<span style="color:var(--blue)">→ 平稳</span>';
var jumps=[];
for(var i=1;i<n;i++){if(Math.abs(nums[i]-nums[i-1])>sd*3)jumps.push({idx:i,from:nums[i-1].toFixed(4),to:nums[i].toFixed(4),time:recs[i].time})}
html+='<table style="font-size:12px;margin-top:6px"><tr><td style="color:var(--subtle)">最小值</td><td>'+min.toFixed(4)+'</td><td style="color:var(--subtle)">最大值</td><td>'+max.toFixed(4)+'</td></tr>';
html+='<tr><td style="color:var(--subtle)">平均值</td><td>'+avg.toFixed(4)+'</td><td style="color:var(--subtle)">中位数</td><td>'+md.toFixed(4)+'</td></tr>';
html+='<tr><td style="color:var(--subtle)">标准差</td><td>'+sd.toFixed(4)+'</td><td style="color:var(--subtle)">趋势</td><td>'+tr+'</td></tr>';
html+='<tr><td style="color:var(--subtle)">首值</td><td>'+first.toFixed(4)+'</td><td style="color:var(--subtle)">末值</td><td>'+last.toFixed(4)+'</td></tr></table>';
if(jumps.length){
html+='<div style="margin-top:6px;font-size:11px;color:var(--red)">突变点('+jumps.length+'): ';
jumps.forEach(function(j){html+=j.time+' ['+j.from+'→'+j.to+']; '});
html+='</div>'
}
}else{
var stats={};
vals.forEach(function(v){stats[v]=(stats[v]||0)+1});
var chg=[];
for(var i=1;i<vals.length;i++){if(vals[i]!==vals[i-1])chg.push({time:recs[i].time,from:vals[i-1],to:vals[i]})}
var dist=Object.keys(stats).map(function(k){var lo=k.toLowerCase();return (lo==='enable'||lo==='on')?'<span class="badge enable">'+k+':'+stats[k]+'次</span>':(lo==='disable'||lo==='off')?'<span class="badge disable">'+k+':'+stats[k]+'次</span>':k+':'+stats[k]+'次'}).join(' ');
html+='<div style="font-size:12px;margin-top:4px">分布: '+dist+'</div>';
html+='<div style="font-size:12px;margin-top:4px">切换次数: '+chg.length+'</div>';
if(chg.length){
html+='<div style="margin-top:4px;font-size:11px;color:var(--red)">变化点: ';
chg.forEach(function(c){html+=c.time+' ['+c.from+'→'+c.to+']; '});
html+='</div>'
}
}
html+='</div>'
})
});
$('analysisBox').innerHTML=html||'<p style="color:var(--subtle)">暂无数据可分析</p>'
}
function isNum(v){if(v===null||v===undefined||v==='')return false;return!isNaN(parseFloat(v))&&isFinite(v)}
function hasNumericField(parsed){if(!parsed||!parsed.length)return false;var row=parsed[0];return Object.keys(row).some(function(c){return isNum(row[c])})}
var chartMode=null;
var chartPoints=[];
var chartCol=null;
function initChart(reset){
var ctr=$('chartBox');
if(reset){if(chart){chart.dispose();chart=null}chartPoints=[];chartCol=null;chartMode=null;ctr.innerHTML=''}
}
function addChartPoint(d){
if(!sel)return;
var r=d[sel];if(!r||!r.parsed||!r.parsed.length)return;
var numeric=hasNumericField(r.parsed);
var ctr=$('chartBox');
if(!numeric){
if(chartMode==='e'&&chart){chart.dispose();chart=null}
chartMode='t';chartPoints=[];chartCol=null;
fetch('http://localhost:8080/api/history?command='+sel+'&limit=500').then(function(resp){return resp.json()}).then(function(h){
var cols=Object.keys(r.parsed[0]),tbl='<table style="font-size:12px"><thead><tr><th>时间</th>';
cols.forEach(function(c){tbl+='<th>'+c+'</th>'});tbl+='</tr></thead><tbody>';
(h||[]).reverse().forEach(function(rec){
if(!rec.parsed||!rec.parsed.length)return;
rec.parsed.forEach(function(row){
var t=rec.time&&rec.time.length>16?rec.time.substring(11,19):(rec.time||'');
tbl+='<tr><td style="color:var(--subtle);font-size:11px">'+t+'</td>';
cols.forEach(function(c){var v=String(row[c]||''),lo=v.toLowerCase();tbl+=['enable','disable','on','off'].indexOf(lo)>=0?'<td><span class="badge '+lo+'">'+v+'</span></td>':'<td>'+v+'</td>'});
tbl+='</tr>'
})
});
tbl+='</tbody></table>';
ctr.innerHTML='<div style="max-height:300px;overflow-y:auto">'+tbl+'</div>'
});
return
}
if(chartMode!=='e'){
if(chart){chart.dispose();chart=null}ctr.innerHTML='';chart=echarts.init(ctr);chartMode='e';chartPoints=[];chartCol=null
}
var row=r.parsed[0];
if(!chartCol){
var numCols=Object.keys(row).filter(function(c){return isNum(row[c])});
chartCol=numCols[0];if(!chartCol)return
}
var val=row[chartCol];
if(!isNum(val))return;
var t=r.time&&r.time.length>16?r.time.substring(11,19):(rec.time||'');
if(chartPoints.length>0&&chartPoints[chartPoints.length-1].time===t)chartPoints.pop();
chartPoints.push({time:t,val:Number(val)});
if(chartPoints.length>200)chartPoints=chartPoints.slice(-200);
chart.setOption({
tooltip:{trigger:'axis'},grid:{left:55,right:20,top:30,bottom:25},
xAxis:{type:'category',data:chartPoints.map(function(p){return p.time}),axisLabel:{color:'#64748b',fontSize:10}},
yAxis:{type:'value',axisLabel:{color:'#94a3b8',fontSize:11}},
series:[{name:chartCol,type:'line',data:chartPoints.map(function(p){return p.val}),smooth:true,symbol:'circle',symbolSize:4,lineStyle:{width:2,color:'#3b82f6'},itemStyle:{color:'#3b82f6'},areaStyle:{color:'rgba(59,130,246,0.12)'},markLine:{data:[{type:'average',name:'avg'}],symbol:'none',lineStyle:{color:'#22c55e',type:'dashed'}}}]})}
function renderFullChart(){
if(!sel){alert('请先启动采集');return}
var r=data[sel];if(!r||!r.parsed||!r.parsed.length)return;
var numeric=hasNumericField(r.parsed);
var ctr=$('chartBox');
if(!numeric){alert('当前命令为非数值型数据');return}
var row=r.parsed[0];
var numCols=Object.keys(row).filter(function(c){return isNum(row[c])});
var col=numCols[0];if(!col){alert('无数值字段');return}
$('chartBox').innerHTML='<p style="color:var(--muted);font-size:13px;padding:20px">正在加载全量数据...</p>';
fetch('http://localhost:8080/api/history?command='+sel+'&limit=200000').then(function(resp){return resp.json()}).then(function(h){
if(!h||!h.length){$('chartBox').innerHTML='<p style="color:var(--subtle);font-size:13px;padding:20px">无数据</p>';return}
var times=[],vals=[];
h.forEach(function(rec){
if(!rec.parsed||!rec.parsed.length)return;
var v=rec.parsed[0][col];if(!isNum(v))return;
var t=rec.time&&rec.time.length>16?rec.time.substring(11,19):(rec.time||'');
times.push(t);vals.push(Number(v))
});
if(!vals.length){$('chartBox').innerHTML='<p style="color:var(--subtle);font-size:13px;padding:20px">无有效数据</p>';return}
if(chart){chart.dispose();chart=null}
chart=echarts.init(ctr);
chart.setOption({
title:{text:'全量数据 ('+vals.length+'条)',textStyle:{color:'#94a3b8',fontSize:12,fontWeight:400},left:'center',top:5},
tooltip:{trigger:'axis'},
grid:{left:55,right:20,top:40,bottom:30},
xAxis:{type:'category',data:times,axisLabel:{color:'#64748b',fontSize:9,interval:Math.floor(times.length/20)||0}},
yAxis:{type:'value',axisLabel:{color:'#94a3b8',fontSize:11}},
series:[{name:col,type:'line',data:vals,large:true,smooth:true,symbol:'none',lineStyle:{width:1,color:'#3b82f6'},areaStyle:{color:'rgba(59,130,246,0.12)'},markLine:{data:[{type:'average',name:'avg'}],symbol:'none',lineStyle:{color:'#22c55e',type:'dashed'}}],
dataZoom:[{type:'inside',start:0,end:100},{type:'slider',start:0,end:100,height:20,bottom:5}]
});
$('chartBox').style.height='380px';chart.resize()
}).catch(function(){$('chartBox').innerHTML='<p style="color:var(--red);font-size:13px;padding:20px">加载失败</p>'})
}
window.addEventListener('resize',function(){if(chart)chart.resize()});
function showHelp(){document.getElementById('helpModal').classList.add('show')}
function hideHelp(){document.getElementById('helpModal').classList.remove('show')}
pollLoop()
</script>
<div class="modal-overlay" id="helpModal" onclick="if(event.target==this)hideHelp()">
<div class="modal">
<button class="modal-close" onclick="hideHelp()">&times;</button>
<h2>使用说明</h2>
<h4>一、基本流程</h4>
<ol>
<li>填写设备 IP、端口、SSH 密码</li>
<li>填写要执行的命令（命令1必填，命令2可选）</li>
<li>设置采集间隔（秒）和时长（0=无限）</li>
<li>点击"启动采集"，页面将实时展示数据和图表</li>
</ol>
<h4>二、命令格式</h4>
<p>命令可以直接填写设备 CLI 命令，例如：</p>
<pre>get-if-rx-curesn0:@bid=3&amp;pid=1&amp;chid=1</pre>
<h4>三、解析字段</h4>
<p>指定从命令返回的表格中提取哪些列，多个用逗号分隔。例如：</p>
<pre>ESN0,EBN0</pre>
<p>留空则提取所有列。</p>
<h4>四、行过滤</h4>
<p>按 <code>key=value</code> 格式过滤行，只保留匹配的行。例如：</p>
<pre>BID=3</pre>
<h4>五、数据来源说明</h4>
<p>页面启动后会自动连接 SSE 接口。如果当前没有运行中的采集任务，会尝试从 <code>data/</code> 目录加载磁盘上已有的历史 JSON 数据文件进行展示。</p>
<h4>六、离线数据导入</h4>
<p>可以直接在 data 目录放置 JSON 文件。文件名即命令名，格式为：</p>
<pre>[
  {
    "time": "2026-06-10 00:00:00",
    "command": "命令名",
    "parsed": [{"字段1": "值1", "字段2": "值2"}],
    "raw": "原始返回文本"
  }
]</pre>
<h4>七、其他功能</h4>
<ul>
<li><strong>实时图表</strong>：自动绘制最新采集的数据折线图</li>
<li><strong>查看全量</strong>：基于全部历史数据绘制完整图表</li>
<li><strong>数据分析</strong>：对历史数据进行统计（最大值、最小值、平均值、波动率）</li>
<li><strong>命令执行记录</strong>：查看所有命令的历史执行记录，支持复制</li>
</ul>
</div>
</div>
</body>
</html>'''


# ======================= 入口 =======================

def main():
    class ThreadedServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
        daemon_threads = True
    server = ThreadedServer(('127.0.0.1', PORT), H)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    print('后台服务已启动')

    url = f'http://localhost:{PORT}'
    print(f'正在打开浏览器: {url}')
    import webbrowser
    webbrowser.open(url)
    print('\n  浏览器已自动打开配置页面\n  关闭此窗口按 Ctrl+C 停止\n')

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print('\n已停止')
        S.stop.set()
        server.shutdown()


if __name__ == '__main__':
    main()
