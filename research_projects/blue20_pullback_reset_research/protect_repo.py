"""Verify remote base tree without importing or modifying protected algorithms."""
from pathlib import Path
import hashlib,json,subprocess
HERE=Path(__file__).resolve().parent
def main():
    frozen=json.loads((HERE/'reference/REMOTE_TREE_BEFORE.json').read_text(encoding='utf-8'))
    raw=subprocess.check_output(['git','ls-tree','-r','-z','HEAD']).decode('utf-8');current={}
    for line in raw.split('\0'):
        if not line:continue
        left,path=line.split('\t',1);current[path]=left.split()[2]
    workflow='.github/workflows/stockhunter_r1d98_state_structure_research_only.yml'
    count=0
    for r in frozen['files']:
        if r['path']==workflow:continue
        assert current.get(r['path'])==r['git_blob_sha1'],('PROTECTED_GIT_BLOB_CHANGED',r['path']);count+=1
    now=Path(workflow).read_bytes().decode('utf-8');eol='\r\n' if '\r\n' in now else '\n';prefix=now.split(eol+'  blue20-pullback-reset-research-only:',1)[0].rstrip('\r\n')+eol
    restored=prefix.replace('          - blue20_pullback_reset'+eol,'',1).replace(" inputs.study != 'blue20_pullback_reset' &&",'',1)
    expected=next(r['git_blob_sha1'] for r in frozen['files'] if r['path']==workflow)
    b=restored.encode('utf-8');actual=hashlib.sha1(b'blob '+str(len(b)).encode()+b'\0'+b).hexdigest();assert actual==expected,('WORKFLOW_BASE_CHANGED',actual,expected)
    dirty=subprocess.check_output(['git','diff','--name-only'],text=True).splitlines()
    assert not [p for p in dirty if not p.startswith('research_projects/blue20_pullback_reset_research/')],dirty
    out=HERE/'results';out.mkdir(exist_ok=True);result=dict(status='PASS',protected_blob_files=count,workflow_existing_body_preserved=True,base_head=frozen['head'])
    (out/'REPOSITORY_PROTECTION.json').write_text(json.dumps(result,indent=2),encoding='utf-8');print(json.dumps(result))
if __name__=='__main__':main()
