from pathlib import Path
import subprocess,json,hashlib
H=Path(__file__).resolve().parent
f=json.loads((H/'REMOTE_TREE.json').read_text());raw=subprocess.check_output(['git','ls-tree','-r','-z','HEAD']).decode('utf-8');current={}
for x in raw.split('\0'):
 if x:
  left,path=x.split('\t',1);current[path]=left.split()[2]
wf='.github/workflows/stockhunter_r1d98_state_structure_research_only.yml'
for path,sha in f['blobs'].items():
 if path!=wf:assert current.get(path)==sha,('PROTECTED_FILE_CHANGED',path)
now=Path(wf).read_bytes().decode('utf-8');eol='\r\n' if '\r\n' in now else '\n';base=now.split(eol+'  blue20-bb-pullback-resistance-research-only:',1)[0].rstrip('\r\n')+eol
base=base.replace('          - blue20_bb_pullback_resistance'+eol,'',1).replace(" inputs.study != 'blue20_bb_pullback_resistance' &&",'',1)
b=base.encode();assert hashlib.sha1(b'blob '+str(len(b)).encode()+b'\0'+b).hexdigest()==f['blobs'][wf]
dirty=subprocess.check_output(['git','diff','--name-only'],text=True).splitlines();assert not [p for p in dirty if not p.startswith('research_projects/blue20_bb_pullback_resistance_research/')],dirty
out=H/'results';out.mkdir(exist_ok=True);(out/'REPOSITORY_PROTECTION.json').write_text(json.dumps(dict(status='PASS',protected_files=len(f['blobs'])-1,old_workflow_restored=True,base=f['base_head']),indent=2),encoding='utf-8');print('protected files and old workflow PASS')
