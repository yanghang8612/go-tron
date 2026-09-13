"""Local fake-systemd fault tests. Never opens a DB or invokes a service."""
import base64
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import types
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, ROOT / path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


m = load('shared_ops', 'scripts/deploy_history_sharing_20260913.py')
s = load('shared_guard', 'scripts/history_shared_reader_guard.py')
GATE = load('shared_pinned_gate', 'scripts/deploy_history_pack_gate_20260913.py')


def command(binary='/candidate/gtron', flag='false'):
    return m.serialize({'path': binary, 'argv[]': binary + ' ' + m.FLAG + flag + ' --datadir /data/main',
                        'ignore_errors': 'no'})


class GuardTests(unittest.TestCase):
    def test_marker_absent_requires_explicit_reader_only(self):
        self.assertFalse(s.check_command(command(), '/candidate/gtron', 'a'*64, 'b'*40, None))
        with self.assertRaises(RuntimeError):
            s.check_command(command(flag='true'), '/candidate/gtron', 'a'*64, 'b'*40, None)

    def test_marker_allows_both_modes_but_never_old_reader(self):
        marker = s.reader_marker('/candidate/gtron', 'a'*64, 'b'*40)
        for enabled in ('true', 'false'):
            self.assertEqual(s.check_command(command(flag=enabled), '/candidate/gtron', 'a'*64, 'b'*40, marker), enabled == 'true')
        with self.assertRaises(RuntimeError):
            s.check_command(command('/old/gtron'), '/candidate/gtron', 'a'*64, 'b'*40, None)
        marker['binary_sha256'] = 'c'*64
        with self.assertRaises(RuntimeError):
            s.check_command(command(), '/candidate/gtron', 'a'*64, 'b'*40, marker)

    def test_missing_duplicate_or_abbreviated_flag_rejected(self):
        for value in (command().replace(m.FLAG+'false', ''),
                      command().replace(m.FLAG+'false', m.FLAG+'false '+m.FLAG+'true'),
                      command().replace(m.FLAG+'false', m.FLAG[:-1]+' false')):
            with self.assertRaises(RuntimeError):
                s.check_command(value, '/candidate/gtron', 'a'*64, 'b'*40, None)

    def test_multiple_precommands_preserve_order_ignore_only_runtime(self):
        a, b = command('/bin/first'), command('/bin/second')
        value = a + ' ; ' + b
        changed = value.replace('pid=0', 'pid=4242').replace('code=(null)', 'code=killed').replace('status=0', 'status=11/SEGV').replace('stop_time=[n/a]', 'stop_time=[Sun 2026-09-13 10:30:00 UTC]')
        self.assertEqual(s.parse_commands(value), s.parse_commands(changed))
        self.assertEqual([x['path'] for x in s.parse_commands(value)], ['/bin/first', '/bin/second'])
        self.assertEqual(s.parse_commands(''), [])

    def test_unknown_or_truncated_command_state_rejected(self):
        for value in (command()[:-1], command()+' trailing', command().replace('pid=0', 'unknown=0'),
                      command().replace('pid=0', 'pid=0 ; pid=1'), command()+' junk '+command()):
            with self.assertRaises(RuntimeError): s.parse_commands(value)

    def test_old_systemctl_repeated_pre_properties_preserve_all_commands(self):
        first,second=command('/bin/space-guard'),command('/usr/bin/python3')
        raw='ActiveState=inactive\nExecStartPre='+first+'\nExecStartPre='+second+'\nMainPID=0\n'
        parsed=m.systemd_properties(raw,s.parse_commands)
        self.assertEqual(s.parse_commands(parsed['ExecStartPre']),s.parse_commands(first+' ; '+second))
        self.assertEqual(parsed['MainPID'],'0')
        self.assertEqual(parsed['ActiveState'],'inactive')
        self.assertEqual(m.systemd_properties('ExecStartPre=\n',s.parse_commands)['ExecStartPre'],'')
        with self.assertRaises(RuntimeError):
            m.systemd_properties('ExecStartPre=\nExecStartPre='+first,s.parse_commands)

    def test_repeated_start_commands_are_not_silently_dropped(self):
        value=command()
        parsed=m.systemd_properties('ExecStart='+value+'\nExecStart='+value,s.parse_commands)
        with self.assertRaisesRegex(RuntimeError,'one mandatory ExecStart'):
            s.check_command(parsed['ExecStart'],'/candidate/gtron','a'*64,'b'*40,None)


class DeploymentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # All imports are fixed local Git blobs; no HTTP/service calls occur.
        with patch.object(GATE, 'REPO', ROOT):
            cls.i, cls.h = GATE.load_modules(ROOT / 'scripts/deploy_range_scheduling_20260913.py')
        cls.i.normalize_exec_start = lambda value: s.parse_commands(value)

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.release = self.root / 'release'
        self.release.mkdir()
        locations = {'RELEASE': self.release, 'BINARY': self.release/'gtron',
                     'GUARD': self.root/'guard.py', 'DROPIN': self.root/'zz-reader.conf',
                     'MARKER': self.root/'marker.json', 'ARMED': self.release/'reader-required.json',
                     'CURRENT_EXE': str(self.root/'old-gtron')}
        for name, value in locations.items():
            p = patch.object(m, name, value); p.start(); self.addCleanup(p.stop)
        self.unit = self.root/'gtron.service'
        self.original = ('[Service]\nExecStart='+m.CURRENT_EXE+' --datadir /data/main\nEnvironment=FIVE=1\n').encode()
        self.off = self.original.replace(m.CURRENT_EXE.encode(), (str(m.BINARY)+' '+m.FLAG+'false').encode())
        self.on = self.off.replace((m.FLAG+'false').encode(), (m.FLAG+'true').encode())
        self.unit.write_bytes(self.original)
        Path(m.CURRENT_EXE).write_bytes(b'old binary')
        m.BINARY.write_bytes(b'new reader')
        self.old = {'binary_sha256': m.sha(b'old binary'), 'prepared_sha256': 'e'*64}
        self.before = {'main': {'properties': {
            'ExecStart': m.serialize({'path': m.CURRENT_EXE, 'argv[]':m.CURRENT_EXE+' --datadir /data/main','ignore_errors':'no'}),
            'ExecStartPre': '', 'FragmentPath': str(self.unit), 'DropInPaths': '',
            'Environment': 'FIVE=1', 'User':'java-tron', 'MemoryLimit':'42949672960',
            'WorkingDirectory':'/data/gtron/main'}, 'files':[m.file_record(self.unit,self.original)]},
            'others': {u: {'files':[]} for u in self.h.PRESERVED_UNITS},
            'holds': {self.h.GLOBAL_HOLD:{'exists':True},self.h.MAIN_HOLD:{'exists':False}},
            'guard_config':{},'guard_script':{},
            'process':{'argv':[m.CURRENT_EXE,'--datadir','/data/main'],'pid':10,'start_ticks':20}}
        self.props = copy.deepcopy(self.before['main']['properties'])
        self.props.update(ActiveState='active',MainPID='10')
        self.holds = copy.deepcopy(self.before['holds'])
        self.guard_blob = b'approved guard'
        self.args = types.SimpleNamespace(mode='bridge',prepared_sha='f'*64,script_revision='d'*40)
        self.record = {'shared_prepared':True,'original_native':self.old,'guard_sha256':m.sha(self.guard_blob),
                       'gate_record_sha256':'1'*64,'shared_tests_sha256':'2'*64,'preflight':self.before,
                       'script_commit':'d'*40,'source_commit':'c'*40,'binary_sha256':m.sha(b'new reader')}
        (self.release/'reader-guard.py').write_bytes(self.guard_blob)
        def edit(before): return before['main']['files'][0],self.original,self.off
        def expected(unused, before, item, data):
            result=copy.deepcopy(before)
            result['main']['files'][0]=m.file_record(self.unit,data)
            result['main']['properties']['ExecStart']=m.serialize({'path':str(m.BINARY),'argv[]':str(m.BINARY)+' '+m.FLAG+'false --datadir /data/main','ignore_errors':'no'})
            return result
        self.g = types.SimpleNamespace(effective_exec_edit=edit,candidate_configuration=expected,verify_prepared=lambda *args: self.record)
        self.calls=[]
        self.failure=None
        self.failed=False
        self.running_exe=m.CURRENT_EXE
        def file_sha(path):
            if str(path).endswith('shared-prepared.json'): return 'f'*64
            if str(path).endswith('gate-prepared.json'): return '1'*64
            if str(path).endswith('native-shared-lifecycle-tests.log'): return '2'*64
            return m.sha(Path(path).read_bytes())
        def saved(path):
            if str(path) in (self.h.GUARD_CONFIG,self.h.GUARD): return {}
            return m.file_record(path,Path(path).read_bytes(),0o755 if Path(path)==m.GUARD else 0o644)
        self.saved=saved
        def atomic(path,data,metadata=None):
            Path(path).write_bytes(data)
            self.calls.append(('write',str(path)))
            if self.failure==str(path) and not self.failed:
                self.failed=True
                raise OSError('injected post-replace fsync failure')
        def run(argv,**kwargs):
            step=argv[1]; self.calls.append((step,))
            if step=='stop': self.props.update(ActiveState='inactive',MainPID='0')
            if step=='daemon-reload':
                if self.failure=='reload' and not self.failed:
                    self.failed=True
                    raise OSError('injected reload failure')
                mode=self.d.current_mode()
                self.props.update(copy.deepcopy(self.d.configs[mode]['main']['properties']))
                self.props['DropInPaths']=str(m.DROPIN) if m.DROPIN.exists() else ''
            if step=='start':
                if self.holds[self.h.MAIN_HOLD]['exists']: raise RuntimeError('real disk latch')
                self.props.update(ActiveState='active',MainPID='22')
                self.running_exe=m.CURRENT_EXE if self.d.current_mode()=='old' else str(m.BINARY)
            return '',0
        self.patch_h('file_sha',file_sha)
        self.patch_h('saved_file',saved)
        self.patch_h('load_json',lambda name:self.record)
        self.patch_h('show',lambda unit:copy.deepcopy(self.props))
        self.patch_h('unit_snapshot',lambda unit:copy.deepcopy(self.before['others'][unit]))
        self.patch_h('hold_snapshot',lambda path:copy.deepcopy(self.holds[path]))
        self.patch_h('atomic_write',atomic)
        self.patch_h('run',run)
        self.patch_h('guard_check',lambda: m.require(not self.holds[self.h.MAIN_HOLD]['exists'],'real disk latch'))
        self.patch_h('save_json',lambda name,record:self.calls.append(('save',name)))
        self.patch_h('process_identity',lambda pid:{'exe':self.running_exe,'exe_sha256':file_sha(self.running_exe)})
        p=patch.object(m,'guard_parent',lambda:None);p.start();self.addCleanup(p.stop)
        p=patch.object(s,'read_root_file',lambda path,limit:Path(path).read_bytes());p.start();self.addCleanup(p.stop)
        self.d=m.Deployment(self.g,self.i,self.h,s,self.guard_blob,self.old,self.args)
        self.d.healthy_mode=lambda mode: {'healthy':mode}
        self.d.admit=lambda: None

    def patch_h(self,name,value):
        p=patch.object(self.h,name,value);p.start();self.addCleanup(p.stop)

    def install_bridge(self):
        self.d.stop();self.d.install_mode('off');self.d.start_candidate()

    def arm(self):
        data=json.dumps(self.d.marker).encode()
        m.ARMED.write_bytes(data);m.MARKER.write_bytes(data)

    def test_bridge_success_installs_reader_only_and_pre_guard(self):
        result=GATE.activate_transaction(self.d)
        self.assertTrue(result['active'])
        self.assertEqual(self.d.current_mode(),'off')
        self.assertFalse(m.marker_required())
        self.assertEqual(len(s.parse_commands(self.props['ExecStartPre'])),1)
        self.d.compare('off')

    def test_every_partial_install_recovers_original_before_arming(self):
        for failure in (str(m.GUARD),str(m.DROPIN),str(self.unit),'reload'):
            with self.subTest(failure=failure):
                self.failure=failure;self.failed=False
                result=GATE.activate_transaction(self.d)
                self.assertEqual(result['phase'],'rolled-back',result)
                self.assertEqual(self.d.current_mode(),'old')
                self.assertEqual(self.props['ActiveState'],'active')
                self.assertFalse(m.GUARD.exists());self.assertFalse(m.DROPIN.exists())

    def test_partial_rollback_with_cached_removed_dropin_is_recoverable(self):
        self.install_bridge()
        self.failure='reload';self.failed=False
        with self.assertRaises(OSError):self.d.install_mode('old')
        self.assertIn(str(m.DROPIN),self.props['DropInPaths'])
        self.assertFalse(m.DROPIN.exists())
        self.assertEqual(self.d.rollback()['healthy'],'old')
        self.assertEqual(self.props['ActiveState'],'active')

    def test_after_arm_failure_recovers_same_reader_off_only(self):
        self.install_bridge();self.arm()
        self.args.mode='enable';self.d.target='on'
        self.failure='reload';self.failed=False
        result=GATE.activate_transaction(self.d)
        self.assertEqual(result['phase'],'rolled-back',result)
        self.assertEqual(self.d.current_mode(),'off')
        self.assertTrue(m.ARMED.exists());self.assertTrue(m.MARKER.exists())
        self.assertTrue(m.GUARD.exists())
        with self.assertRaises(RuntimeError):self.d.install_mode('old')

    def test_each_marker_alone_permanently_prohibits_old_rollback(self):
        self.install_bridge()
        for path in (m.ARMED,m.MARKER):
            with self.subTest(marker=path):
                path.write_text(json.dumps(self.d.marker))
                self.assertEqual(self.d.rollback()['healthy'],'off')
                with self.assertRaises(RuntimeError):self.d.install_mode('old')
                path.unlink()  # Test fixture only; no production remove operation.

    def test_unknown_argv_or_environment_fails_even_during_recovery(self):
        for prop in ('ExecStart','Environment'):
            before=self.props[prop]
            self.props[prop]=before.replace('/data/main','/data/other') if prop=='ExecStart' else 'FIVE=0'
            with self.assertRaises(RuntimeError):self.d.compare('old',transitional=True)
            self.props[prop]=before

    def test_unapproved_guard_bytes_rejected(self):
        self.install_bridge();m.GUARD.write_bytes(b'unknown')
        with self.assertRaises(RuntimeError):self.d.rollback()

    def test_real_latch_preserved_config_recovery_but_blocks_start(self):
        self.install_bridge();self.arm()
        self.holds[self.h.MAIN_HOLD]={'exists':True,'reason':'space guard'}
        with self.assertRaisesRegex(RuntimeError,'disk latch'):self.d.rollback()
        self.assertEqual(self.d.current_mode(),'off')
        self.assertTrue(self.holds[self.h.MAIN_HOLD]['exists'])
        self.assertTrue(m.MARKER.exists())

    def test_global_hold_change_never_accepted(self):
        self.holds[self.h.GLOBAL_HOLD]={'exists':False}
        with self.assertRaises(RuntimeError):self.d.compare('old',transitional=True,allow_latch=True)

    def test_stop_rejects_unrelated_process(self):
        self.running_exe=str(self.root/'unrelated');Path(self.running_exe).write_bytes(b'other')
        with self.assertRaises(RuntimeError):self.d.stop(recovering=True)
        self.assertNotIn(('stop',),self.calls)

    def test_marker_write_failure_does_not_stop_service_or_undo_arm(self):
        self.install_bridge()
        self.d.admit=m.Deployment.admit.__get__(self.d)
        self.args.mode='enable';self.failure=str(m.MARKER);self.failed=False
        start=len(self.calls)
        with self.assertRaises(OSError):GATE.activate_transaction(self.d)
        self.assertNotIn(('stop',),self.calls[start:])
        self.assertTrue(m.marker_required());self.assertEqual(self.d.current_mode(),'off')

    def test_prepared_ops_revision_is_pinned(self):
        self.args.script_revision='e'*40
        with self.assertRaisesRegex(RuntimeError,'ops revision'):
            m.Deployment(self.g,self.i,self.h,s,self.guard_blob,self.old,self.args)


class ModuleAdmissionTests(unittest.TestCase):
    def load(self, record_sha='e'*64):
        args=types.SimpleNamespace(current_pid=123,current_start_ticks=456,current_record_sha=record_sha,
                                   script_revision='d'*40)
        real_module=m.module_from_blob
        def module(name, blob, filename):
            if name=='native_record_verifier':
                return types.SimpleNamespace(verify_native_record=lambda *args:{'prepared_sha256':'e'*64,'binary_sha256':'a'*64})
            return real_module(name,blob,filename)
        def blob(revision,path):
            if path in (m.GUARD_SOURCE,m.NATIVE_RECORD_HELPER):return (ROOT/path).read_bytes()
            return subprocess.check_output(['git','show',revision+':'+path],cwd=ROOT)
        with patch.object(m,'APPROVED',True),patch.object(m,'ALLOWED_FILES',('core/example.go',)),\
                patch.object(m,'REPO',ROOT),patch.object(m,'git_blob',side_effect=blob),\
                patch.object(m,'module_from_blob',side_effect=module):
            return m.load_modules(args,ROOT/'scripts/deploy_range_scheduling_20260913.py')

    def test_real_loaded_builder_uses_current_b130_as_diff_base(self):
        g,i,h,guard,_,_=self.load()
        source,ops='c'*40,'d'*40
        commands=[]
        def run(argv,**kwargs):
            commands.append(argv)
            if argv[1]=='rev-parse':
                ref=argv[-1].removesuffix('^{commit}')
                return {'HEAD':i.CHECKOUT,'refs/remotes/origin/master':ops}.get(ref,ref)+'\n',0
            if argv[1]=='diff':return 'M\tcore/example.go\nM\tdocs/example.md\n',0
            if argv[1]=='status':return ' M third_party/librustzcash\n',0
            if argv[1]=='merge-base':return '',0
            raise AssertionError(argv)
        script=Path(m.__file__).read_bytes()
        with patch.object(h,'run',side_effect=run),patch.object(h,'file_sha',return_value=m.sha(script)),\
                patch.object(i.subprocess,'check_output',side_effect=lambda argv,**kwargs:script if argv[-1].endswith(m.SCRIPT_PATH) else b'source'):
            result=i.validate_revision(h,source,ops)
        self.assertEqual(i.CURRENT_SOURCE,m.CURRENT_SOURCE)
        self.assertEqual(result['base_commit'],'b1305f553b5639d90eb2f58c6a5a1b23b9b025b5')
        self.assertIn(['git','diff','--name-status','--no-renames',m.CURRENT_SOURCE,source,'--'],commands)
        self.assertEqual(g.CURRENT_EXE,m.CURRENT_EXE)
        self.assertEqual(h.OLD_PID,123)
        self.assertEqual(h.OLD_START_TICKS,456)
        for binary in (m.CURRENT_EXE,str(m.BINARY)):
            self.assertEqual(h.expected_history_queue_environment(binary),'1')
            self.assertEqual(h.expected_history_range_environment(binary),'1')
        self.assertIn('HistorySharing',i.NATIVE_TEST_PATTERN)

    def test_original_record_changed_rejected(self):
        with self.assertRaisesRegex(RuntimeError,'original native record changed'):self.load('f'*64)

    def test_loaded_show_aggregates_original_and_reader_pre_guard(self):
        _,_,h,_,_,_=self.load()
        first,second=command('/bin/space-guard'),command('/usr/bin/python3')
        raw='ExecStartPre='+first+'\nExecStartPre='+second+'\n'
        with patch.object(h,'run',return_value=(raw,0)):
            self.assertEqual(s.parse_commands(h.show(h.SERVICE)['ExecStartPre']),
                             s.parse_commands(first+' ; '+second))
        # Preserved unrelated units can use arbitrary shell command syntax;
        # their exact files are checked, without applying main's argv parser.
        with patch.object(h,'run',return_value=('ExecStart=arbitrary shell serialization\n',0)):
            self.assertEqual(h.show('preserved.service')['ExecStart'],'arbitrary shell serialization')

    def test_loaded_wrapper_closures_edit_current_and_preserve_other_argv(self):
        g,i,h,_,_,_=self.load()
        argv=m.CURRENT_EXE+' --datadir /data/gtron/main/datadir --history.block-dedup=true'
        unit=('[Service]\nExecStart='+argv+'\nEnvironment=FIVE=1\n').encode()
        item=m.file_record('/etc/systemd/system/gtron.service',unit)
        before={'main':{'files':[item],'properties':{'ExecStart':m.serialize(
            {'path':m.CURRENT_EXE,'argv[]':argv,'ignore_errors':'no'})}}}
        item,original,updated=g.effective_exec_edit(before)
        expected=g.candidate_configuration(i,before,item,updated)
        self.assertEqual(updated.replace((str(m.BINARY)+' '+m.FLAG+'false').encode(),m.CURRENT_EXE.encode()),original)
        self.assertEqual(s.parse_commands(expected['main']['properties']['ExecStart'])[0],
                         {'path':str(m.BINARY),'argv[]':str(m.BINARY)+' '+m.FLAG+'false'+argv[len(m.CURRENT_EXE):],'ignore_errors':'no'})
        self.assertEqual(h.BINARY,m.BINARY)
        self.assertEqual(i.BINARY,m.BINARY)
        self.assertEqual(g.CURRENT_EXE,i.CURRENT_EXE)

    def test_unknown_fresh_process_identity_rejected_before_loading(self):
        args=types.SimpleNamespace(current_pid=0,current_start_ticks=0,current_record_sha='e'*64)
        with patch.object(m,'APPROVED',True),patch.object(m,'ALLOWED_FILES',('x',)),patch.object(m,'git_blob') as blob:
            with self.assertRaisesRegex(RuntimeError,'fresh native process'):m.load_modules(args)
            blob.assert_not_called()


if __name__=='__main__':unittest.main(verbosity=2)
