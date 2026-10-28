# Resume the terminal failed attempt; its immutable derived account file is kept.
import pathlib,shutil,json,traceback,os
parent=pathlib.Path('/data/gtron/maintenance/first-cold-followup-20261028-01')
code=(parent/'account_gap_ablation.py').read_text()
prefix=code.split('\ntry:\n s[',1)[0].replace(';R.mkdir()',';R.mkdir(exist_ok=True)')
exec(compile(prefix,'account-gap-definitions','exec'))
prior=json.loads((R/'candidate-status.json').read_text());assert prior['stage']=='failed' and prior['original_account_uncovered']==31 and prior['ablation_account_uncovered']==1521
s=prior;s.pop('error',None);s.update(complete=False,stage='resume after private ancient directory layout repair');s['production_before']=identity();requests=[]
try:
 dest=R/'input-1521-gaps';p=dest/'ancient';assert p.is_symlink()
 # Freezer requires a real directory. Drop only our link; never its target.
 p.unlink();shutil.copytree(str(original/'ancient'),str(p),copy_function=os.link,symlinks=True)
 failed=R/'gaps-1';assert failed.is_dir() and not (R/'failed-layout-gaps-1').exists();failed.rename(R/'failed-layout-gaps-1')
 body=code[code.index(" matrix=[('gaps-1'"):code.index('\nexcept BaseException:')]
 exec(compile('if True:\n'+body,'resumed-account-gap-matrix','exec'))
except BaseException:s.update(stage='failed',error=traceback.format_exc());save();raise
