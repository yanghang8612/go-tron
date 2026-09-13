"""Local-only compatible-reader transaction and native module isolation tests."""
import base64
import copy
import importlib.util
import json
from pathlib import Path
import shlex
import signal
import subprocess
import tempfile
import types
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]


def load(name, path):
    spec=importlib.util.spec_from_file_location(name,ROOT/path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


m=load('compatible_cpu_ops','scripts/deploy_history_sharing_cpu_20260913.py')
g=load('compatible_transaction','scripts/deploy_history_pack_gate_20260913.py')
s=load('compatible_guard','scripts/history_shared_reader_guard.py')
w=load('compatible_show','scripts/deploy_history_sharing_systemd_20260913.py')


def command(path, argv):
    return '{ path='+path+' ; argv[]='+argv+' ; ignore_errors=no ; start_time=[n/a] ; stop_time=[n/a] ; pid=0 ; code=(null) ; status=0 }'


def item(path,data,mode=0o644):
    return {'path':str(path),'data_b64':base64.b64encode(data).decode(),'sha256':m.sha(data),
            'mode':mode,'uid':0,'gid':0,'xattrs':{}}


class TransactionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with patch.object(g,'REPO',ROOT):cls.i,cls.base_h=g.load_modules(ROOT/'scripts/deploy_range_scheduling_20260913.py')
        cls.i.normalize_exec_start=s.parse_commands

    def setUp(self):
        tmp=tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup);self.root=Path(tmp.name)
        self.release=self.root/'candidate';self.release.mkdir()
        self.old_release=self.root/'old';self.old_release.mkdir()
        for name,value in (('RELEASE',self.release),('BINARY',self.release/'gtron')):
            p=patch.object(m,name,value);p.start();self.addCleanup(p.stop)
        self.old_exe=self.old_release/'gtron';self.old_exe.write_bytes(b'old v3 reader')
        m.BINARY.write_bytes(b'candidate v3 reader')
        self.old_sha=m.sha(self.old_exe.read_bytes());self.new_sha=m.sha(m.BINARY.read_bytes());self.source='c'*40
        self.unit=self.root/'gtron.service';self.dropin=self.root/'zz-reader.conf';self.guard=self.root/'reader-guard.py'
        self.data_marker=self.root/'data-marker.json';self.old_armed=self.old_release/'reader-required.json'
        old_marker=s.reader_marker(self.old_exe,self.old_sha,m.CURRENT_SOURCE)
        for path in (self.data_marker,self.old_armed):path.write_text(json.dumps(old_marker,sort_keys=True)+'\n')
        self.guard.write_bytes(b'unchanged guard');self.guard.chmod(0o755)
        argv=str(self.old_exe)+' '+m.FLAG+'true --datadir /data/main'
        unit=('[Service]\nExecStart='+argv+'\nEnvironment=FIVE=1\n').encode();self.unit.write_bytes(unit)
        guard_argv='/usr/bin/python3 '+str(self.guard)+' --binary '+str(self.old_exe)+' --sha256 '+self.old_sha+' --source '+m.CURRENT_SOURCE+' --marker '+str(self.data_marker)
        self.dropin.write_text('[Service]\nExecStartPre='+guard_argv+'\n')
        self.props={'ExecStart':command(str(self.old_exe),argv),'ExecStartPre':command('/bin/space-guard','/bin/space-guard pre-start')+' ; '+command('/usr/bin/python3',guard_argv),
                    'Environment':'FIVE=1','WorkingDirectory':'/data/main','User':'java-tron','MemoryLimit':'42949672960',
                    'FragmentPath':str(self.unit),'DropInPaths':str(self.dropin),'ActiveState':'active','MainPID':str(m.CURRENT_PID)}
        bh=self.base_h
        self.holds={bh.GLOBAL_HOLD:{'exists':True},bh.MAIN_HOLD:{'exists':False}}
        self.before={'main':{'properties':copy.deepcopy(self.props),'files':[item(self.unit,unit),item(self.dropin,self.dropin.read_bytes())]},
                     'others':{u:{'files':[]} for u in bh.PRESERVED_UNITS},'holds':copy.deepcopy(self.holds),
                     'guard_config':{},'guard_script':{},'process':{'argv':shlex.split(argv),'pid':m.CURRENT_PID,'start_ticks':m.CURRENT_TICKS}}
        self.record={'current':self.before,'binary_sha256':self.new_sha,'source_commit':self.source,
                     'old_marker':item(self.data_marker,self.data_marker.read_bytes()),'old_armed':item(self.old_armed,self.old_armed.read_bytes()),
                     'reader_guard':item(self.guard,self.guard.read_bytes(),0o755)}
        self.calls=[];self.failure=None;self.failed=False;self.failure_error=OSError('injected after replace')
        self.running_exe=str(self.old_exe)
        def saved(path):
            if str(path) in (bh.GUARD_CONFIG,bh.GUARD):return {}
            return item(path,Path(path).read_bytes(),0o755 if Path(path)==self.guard else 0o644)
        def show(unit):
            raw=[]
            for key,value in self.props.items():
                if key=='ExecStartPre':
                    for entry in s.parse_commands(value):raw.append(key+'='+command(entry['path'],entry['argv[]']))
                else:raw.append(key+'='+value)
            return w.parse_properties('\n'.join(raw),s.parse_commands)
        def snapshot(unit):
            if unit!=bh.SERVICE:return copy.deepcopy(self.before['others'][unit])
            return {'properties':show(unit),'files':[saved(self.unit),saved(self.dropin)]}
        def atomic(path,data,metadata=None):
            Path(path).write_bytes(data);self.calls.append(('write',str(path)))
            if self.failure==str(path) and not self.failed:
                self.failed=True;raise self.failure_error
        def run(argv,**kwargs):
            step=argv[1];self.calls.append((step,))
            if step=='stop':self.props.update(ActiveState='inactive',MainPID='0')
            if step=='daemon-reload':
                if self.failure=='reload' and not self.failed:self.failed=True;raise self.failure_error
                mode=self.d.mode();self.props.update(copy.deepcopy(self.d.configs[mode]['main']['properties']))
                self.props.update(ActiveState='inactive',MainPID='0')
            if step=='start':
                values=shlex.split(s.parse_commands(self.props['ExecStartPre'])[-1]['argv[]'])
                params={values[n]:values[n+1] for n in range(2,len(values),2)}
                s.check_command(self.props['ExecStart'],params['--binary'],params['--sha256'],params['--source'],json.loads(self.data_marker.read_bytes()))
                self.running_exe=params['--binary'];self.props.update(ActiveState='active',MainPID='20000')
            return '',0
        def guard_check():m.require(not self.holds[bh.MAIN_HOLD]['exists'],'real disk latch')
        self.h=types.SimpleNamespace(**{name:getattr(bh,name) for name in ('SERVICE','PRESERVED_UNITS','GLOBAL_HOLD','MAIN_HOLD','GUARD_CONFIG','GUARD','SYSTEMCTL')},
            show=show,unit_snapshot=snapshot,saved_file=saved,file_sha=lambda path:m.sha(Path(path).read_bytes()),
            hold_snapshot=lambda path:copy.deepcopy(self.holds[path]),atomic_write=atomic,run=run,guard_check=guard_check,
            process_identity=lambda pid:{'exe':self.running_exe,'exe_sha256':m.sha(Path(self.running_exe).read_bytes())},
            save_json=lambda name,data:(self.release/name).write_text(json.dumps(data)),same_configuration=bh.same_configuration)
        self.original=types.SimpleNamespace(MARKER=self.data_marker,ARMED=self.old_armed,DROPIN=self.dropin,GUARD=self.guard)
        self.b=types.SimpleNamespace(original=self.original,i=types.SimpleNamespace(CURRENT_EXE=str(self.old_exe),CURRENT_SHA=self.old_sha,same_configuration=self.i.same_configuration),h=self.h,guard=s,g=g)
        self.args=types.SimpleNamespace(mode='activate',rollback_writer='on')
        p=patch.object(m,'verify_prepared',return_value=self.record);p.start();self.addCleanup(p.stop)
        self.d=m.Deployment(self.b,self.args)
        self.d.admit=lambda:self.d.check('old_on')
        self.d.healthy_mode=lambda mode:self.d.check(mode) or {'healthy':mode}
        self.old_marker_bytes=self.data_marker.read_bytes();self.old_armed_bytes=self.old_armed.read_bytes()

    def test_success_updates_reader_identity_and_preserves_guard_old_marker(self):
        guard_bytes=self.guard.read_bytes()
        result=g.activate_transaction(self.d)
        self.assertTrue(result['active'],result)
        self.assertEqual(self.d.mode(),'candidate')
        self.assertEqual(json.loads(self.data_marker.read_bytes())['source_commit'],self.source)
        self.assertEqual(self.guard.read_bytes(),guard_bytes)
        self.assertEqual(self.old_armed.read_bytes(),self.old_armed_bytes)
        self.assertTrue((self.release/'reader-required.json').exists())
        self.assertEqual(self.props['ActiveState'],'active')

    def test_every_file_replace_and_reload_failure_restores_d656_on(self):
        for failure in (str(self.release/'reader-required.json'),str(self.data_marker),str(self.unit),str(self.dropin),'reload'):
            with self.subTest(failure=failure):
                self.failure=failure;self.failed=False
                result=g.activate_transaction(self.d)
                self.assertEqual(result['phase'],'rolled-back',result)
                self.assertEqual(self.d.mode(),'old_on');self.assertEqual(self.props['ActiveState'],'active')
                self.assertEqual(self.data_marker.read_bytes(),self.old_marker_bytes)
                self.assertEqual(self.old_armed.read_bytes(),self.old_armed_bytes)
                self.assertTrue((self.release/'reader-required.json').exists())
                state=json.loads((self.release/'upgrade-state.json').read_text())
                self.assertIn('error',state)

    def test_explicit_rollback_off_keeps_all_permanent_markers(self):
        self.assertTrue(g.activate_transaction(self.d)['active'])
        self.args.mode='rollback';self.args.rollback_writer='off'
        g.protected_rollback(self.d)
        self.assertEqual(self.d.mode(),'old_off')
        self.assertEqual(self.data_marker.read_bytes(),self.old_marker_bytes)
        self.assertTrue((self.release/'reader-required.json').exists())
        self.assertEqual(self.old_armed.read_bytes(),self.old_armed_bytes)

    def test_interrupt_after_marker_replace_restores_with_signals_ignored(self):
        self.failure=str(self.data_marker);self.failure_error=KeyboardInterrupt()
        old_stop=self.d.stop
        def stop():
            if self.failed:self.assertTrue(all(signal.getsignal(sig)==signal.SIG_IGN for sig in (signal.SIGINT,signal.SIGTERM,signal.SIGHUP)))
            old_stop()
        self.d.stop=stop
        self.assertEqual(g.activate_transaction(self.d)['phase'],'rolled-back')
        self.assertEqual(self.d.mode(),'old_on')

    def test_genuine_latch_never_removed_even_if_start_is_blocked(self):
        self.assertTrue(g.activate_transaction(self.d)['active'])
        self.holds[self.h.MAIN_HOLD]={'exists':True,'reason':'disk pressure'}
        with self.assertRaisesRegex(RuntimeError,'disk latch'):g.protected_rollback(self.d)
        self.assertEqual(self.d.mode(),'old_on');self.assertTrue(self.holds[self.h.MAIN_HOLD]['exists'])
        self.assertEqual(self.data_marker.read_bytes(),self.old_marker_bytes)

    def test_unknown_marker_or_guard_change_rejected_before_stop(self):
        for path in (self.data_marker,self.old_armed,self.guard):
            original=path.read_bytes();path.write_bytes(b'unknown')
            with self.assertRaises(RuntimeError):self.d.stop()
            path.write_bytes(original)
        self.assertNotIn(('stop',),self.calls)

    def test_pre_v3_or_unrelated_process_cannot_be_stopped(self):
        path=self.root/'b130';path.write_bytes(b'pre-v3');self.running_exe=str(path)
        with self.assertRaises(RuntimeError):self.d.stop()
        self.assertNotIn(('stop',),self.calls)

    def test_unapproved_effective_precommand_environment_and_global_hold_reject(self):
        for name in ('ExecStart','ExecStartPre','Environment'):
            original=self.props[name]
            self.props[name]=original.replace('/data/main','/other/data') if name=='ExecStart' else original+' unknown'
            with self.assertRaises(RuntimeError):self.d.check(transitional=True)
            self.props[name]=original
        self.holds[self.h.GLOBAL_HOLD]={'exists':False}
        with self.assertRaises(RuntimeError):self.d.check(transitional=True,allow_latch=True)


class ModuleTests(unittest.TestCase):
    def test_frozen_source_scope_is_exact_and_separate_from_ops_adapter(self):
        output=subprocess.check_output(['git','diff','--name-only',m.CURRENT_SOURCE,m.SOURCE_REVISION,'--'],cwd=ROOT).decode()
        actual={path for path in output.splitlines() if not (path.startswith('docs/') and path.endswith('.md'))}
        self.assertEqual(actual,set(m.ALLOWED_FILES));self.assertEqual(len(actual),9)
        self.assertNotIn(m.SCRIPT_PATH,actual)
        self.assertTrue(m.APPROVED)
        for revision,path,checksum in ((m.WRAPPER_REVISION,m.WRAPPER_PATH,m.WRAPPER_SHA),
                                       (m.BUILDER_REVISION,m.BUILDER_PATH,m.BUILDER_SHA)):
            blob=subprocess.check_output(['git','show',revision+':'+path],cwd=ROOT)
            self.assertEqual(m.sha(blob),checksum)

    def test_candidate_module_rebinding_does_not_touch_old_attestation(self):
        old_h=types.SimpleNamespace(RELEASE=Path('/original/release'),BINARY=Path('/original/gtron'),OLD_EXE='/b130/gtron')
        old_i=types.SimpleNamespace(RELEASE=old_h.RELEASE,CURRENT_SOURCE='b130')
        old=types.SimpleNamespace(record={'binary_sha256':'a'*64})
        original=types.SimpleNamespace(BINARY=Path('/original/gtron'),Deployment=lambda *args:old)
        wrapper=types.SimpleNamespace(load_prepared=lambda *args:(original,'g',old_i,old_h,s,b'guard','native','args'),install_show=lambda *args:None)
        new_h=types.SimpleNamespace(RELEASE=Path('/base'),BINARY=Path('/base/gtron'))
        builder=types.SimpleNamespace(load_helper=lambda:new_h)
        before=(copy.deepcopy(vars(old_h)),copy.deepcopy(vars(old_i)))
        with patch.object(m,'APPROVED',True),patch.object(m,'ALLOWED_FILES',('candidate.go',)),\
                patch.object(m,'pinned_module',side_effect=[wrapper,builder]):
            bundle=m.load(types.SimpleNamespace(old_prepared_sha='b'*64))
        self.assertIs(bundle.h,new_h);self.assertIsNot(bundle.h,old_h)
        self.assertEqual((vars(old_h),vars(old_i)),before)
        self.assertEqual(builder.CURRENT_SOURCE,m.CURRENT_SOURCE)
        self.assertEqual(builder.__file__ if hasattr(builder,'__file__') else None,None)
        self.assertEqual(builder.CURRENT_PID,17289);self.assertEqual(builder.CURRENT_TICKS,4503924631)
        self.assertEqual(new_h.BINARY,m.BINARY)
        self.assertEqual(new_h.expected_history_queue_environment(str(m.BINARY)),'1')
        with self.assertRaises(RuntimeError):new_h.expected_history_queue_environment('/b130/gtron')

    def test_pending_review_refuses_before_any_dependency_execution(self):
        with patch.object(m,'APPROVED',False),patch.object(m,'pinned_module') as loader:
            with self.assertRaises(RuntimeError):m.load(types.SimpleNamespace())
            loader.assert_not_called()


if __name__=='__main__':unittest.main(verbosity=2)
