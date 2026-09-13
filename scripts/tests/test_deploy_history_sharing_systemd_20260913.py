"""Test the operational parser shim without service, filesystem or DB writes."""
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import types
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]


def load(name, path):
    spec=importlib.util.spec_from_file_location(name, ROOT/path)
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


w=load('systemd_writer_shim','scripts/deploy_history_sharing_systemd_20260913.py')
s=load('systemd_writer_guard','scripts/history_shared_reader_guard.py')


def command(path, args=''):
    return '{ path='+path+' ; argv[]='+path+args+' ; ignore_errors=no ; start_time=[n/a] ; stop_time=[n/a] ; pid=0 ; code=(null) ; status=0 }'


class ParserTests(unittest.TestCase):
    def test_real_old_systemctl_shape_keeps_space_and_reader_guards(self):
        first=command('/usr/local/bin/gtron-space-guard',' pre-start mainnet')
        second=command('/usr/bin/python3',' /usr/local/libexec/gtron-history-shared-reader-guard.py --binary /new/gtron')
        raw='ActiveState=inactive\nMainPID=0\nExecStartPre='+first+'\nExecStartPre='+second+'\n'
        expected=s.parse_commands(first+' ; '+second)
        self.assertEqual(s.parse_commands(w.parse_properties(raw,s.parse_commands)['ExecStartPre']),expected)
        # Reproduces the native failure: inherited dict parsing lost command 1.
        old=dict(line.split('=',1) for line in raw.splitlines())
        self.assertNotEqual(s.parse_commands(old['ExecStartPre']),expected)

    def test_repeated_runtime_state_changes_only_metadata(self):
        raw='ExecStartPre='+command('/guard')+'\nExecStartPre='+command('/reader')+'\n'
        newer=raw.replace('pid=0','pid=123').replace('code=(null)','code=exited').replace('stop_time=[n/a]','stop_time=[Sun 2026-09-13 12:00:00 UTC]')
        self.assertEqual(s.parse_commands(w.parse_properties(raw,s.parse_commands)['ExecStartPre']),
                         s.parse_commands(w.parse_properties(newer,s.parse_commands)['ExecStartPre']))

    def test_unknown_or_empty_duplicate_never_ignored(self):
        for raw in ('ExecStartPre=\nExecStartPre='+command('/guard'),
                    'ExecStartPre='+command('/guard').replace('pid=0','unknown=0')):
            with self.assertRaises(RuntimeError):w.parse_properties(raw,s.parse_commands)

    def test_shim_only_changes_main_service_parsing(self):
        raw='ExecStartPre='+command('/first')+'\nExecStartPre='+command('/last')+'\n'
        h=types.SimpleNamespace(SERVICE='gtron.service',SYSTEMCTL='/bin/systemctl',run=lambda *args:(raw,0))
        w.install_show(h,s)
        self.assertEqual(len(s.parse_commands(h.show('gtron.service')['ExecStartPre'])),2)
        self.assertEqual(len(s.parse_commands(h.show('other.service')['ExecStartPre'])),1)


class AttestationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.blob=subprocess.check_output(['git','show',w.ORIGINAL_COMMIT+':'+w.ORIGINAL_PATH],cwd=ROOT)

    def setUp(self):
        self.record={'shared_prepared':True,'source_commit':w.ORIGINAL_COMMIT,'script_commit':w.ORIGINAL_COMMIT,
                     'preflight':{'process':{'pid':8172,'start_ticks':4503717976}},
                     'original_native':{'prepared_sha256':'a'*64}}

    def invoke(self, mode='enable', expected_sha=None, changed_script=False):
        raw=json.dumps(self.record).encode()
        self.loaded={}
        h=types.SimpleNamespace(SERVICE='gtron.service',SYSTEMCTL='/bin/systemctl',run=lambda *args:('',0))
        def execute(code, namespace):
            self.loaded['filename']=namespace['__file__']
            def modules(args):
                self.loaded['args']=args
                return 'g','i',h,s,b'guard','old'
            namespace['load_modules']=modules
        def read(path, limit):
            if Path(path).name=='shared-prepared.json':return raw
            return b'changed' if changed_script else self.blob
        with patch.object(w,'read_root',side_effect=read),patch.object(w,'git',return_value=self.blob),\
                patch.object(w,'exec',side_effect=execute,create=True):
            return w.load_prepared(expected_sha or w.sha(raw),mode)

    def test_preserves_original_source_script_and_prepared_identity(self):
        self.assertEqual(w.sha(self.blob),w.ORIGINAL_SHA)
        result=self.invoke()
        args=result[-1]
        self.assertEqual(self.loaded['filename'],str(w.RELEASE/'source'/w.ORIGINAL_PATH))
        self.assertEqual(args.script_revision,w.ORIGINAL_COMMIT)
        self.assertEqual(args.current_pid,8172)
        self.assertEqual(args.current_start_ticks,4503717976)
        self.assertEqual(args.current_record_sha,'a'*64)
        self.assertEqual(args.mode,'enable')

    def test_no_prepare_bridge_or_old_rollback_mode(self):
        for mode in ('prepare','bridge','rollback','old'):
            with self.assertRaises(RuntimeError):self.invoke(mode)

    def test_reviewed_record_or_script_change_rejected(self):
        with self.assertRaisesRegex(RuntimeError,'record changed'):self.invoke(expected_sha='0'*64)
        with self.assertRaisesRegex(RuntimeError,'deployment script changed'):self.invoke(changed_script=True)

    def test_source_ops_and_original_process_fail_closed(self):
        for key in ('source_commit','script_commit'):
            original=self.record[key];self.record[key]='f'*40
            with self.assertRaises(RuntimeError):self.invoke()
            self.record[key]=original
        self.record['preflight']['process']['pid']=True
        with self.assertRaises(RuntimeError):self.invoke()

    def test_wrapper_requires_its_own_exact_git_blob(self):
        revision='e'*40
        blob=b'wrapper'
        def git(*args):
            if args[0]=='rev-parse':return revision.encode()+b'\n'
            if args[0]=='merge-base':return b''
            if args[0]=='show':return blob
            raise AssertionError(args)
        with patch.object(w,'git',side_effect=git),patch.object(w,'read_root',return_value=blob):
            self.assertEqual(w.verify_wrapper(revision),hashlib.sha256(blob).hexdigest())
        with patch.object(w,'git',side_effect=git),patch.object(w,'read_root',return_value=b'changed'):
            with self.assertRaises(RuntimeError):w.verify_wrapper(revision)


if __name__=='__main__':unittest.main(verbosity=2)
