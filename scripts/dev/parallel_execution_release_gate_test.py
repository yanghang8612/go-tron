import importlib.util
import pathlib
import unittest


SCRIPT = pathlib.Path(__file__).with_name("parallel_execution_release_gate.py")
SPEC = importlib.util.spec_from_file_location("parallel_execution_release_gate", SCRIPT)
GATE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GATE)


def clean_metrics():
    names = [
        "core/mainnet_state_repair/create_transfer_failure",
        "core/mainnet_state_repair/parallel_vm_missed_payment",
        "core/mainnet_state_repair/cost_missed_reward",
        "core/mainnet_state_repair/wink_missing_runtime",
        "core/speculative_execution/safety_fallbacks",
        "core/parallel_transfer/errors",
        "core/parallel_transfer/sender_retry/errors",
        "core/parallel_transfer/balance_oracle/candidates",
        "core/parallel_transfer/balance_oracle/matches",
        "core/parallel_transfer/balance_oracle/fallbacks",
        "core/parallel_transfer/balance_oracle/mismatches",
        "core/parallel_transfer/balance_oracle/errors",
        "core/parallel_transfer/serial_verify/candidates",
        "core/parallel_transfer/serial_verify/matches",
        "core/parallel_transfer/serial_verify/info_mismatches",
        "core/parallel_transfer/serial_verify/write_set_mismatches",
        "core/parallel_transfer/serial_verify/read_set_differences",
        "core/parallel_transfer/serial_verify/balance_trace_mismatches",
        "core/parallel_transfer/serial_verify/restore_mismatches",
        "core/parallel_transfer/serial_verify/errors",
        "core/parallel_transfer/write_seal/candidates",
        "core/parallel_transfer/write_seal/matches",
        "core/parallel_transfer/write_seal/mismatches",
        "core/parallel_transfer/publish_audit/candidates",
        "core/parallel_transfer/publish_audit/matches",
        "core/parallel_transfer/publish_audit/mismatches",
        "core/parallel_transfer/publish_audit/errors",
        "core/parallel_vm/serial_verify/candidates",
        "core/parallel_vm/serial_verify/matches",
        "core/parallel_vm/serial_verify/info_mismatches",
        "core/parallel_vm/serial_verify/write_set_mismatches",
        "core/parallel_vm/serial_verify/read_set_differences",
        "core/parallel_vm/serial_verify/balance_trace_mismatches",
        "core/parallel_vm/serial_verify/restore_mismatches",
        "core/parallel_vm/serial_verify/errors",
        "core/parallel_vm/dual_oracle/candidates",
        "core/parallel_vm/dual_oracle/matches",
        "core/parallel_vm/dual_oracle/info_mismatches",
        "core/parallel_vm/dual_oracle/write_set_mismatches",
        "core/parallel_vm/dual_oracle/read_set_differences",
        "core/parallel_vm/dual_oracle/balance_trace_mismatches",
        "core/parallel_vm/dual_oracle/errors",
        "core/parallel_vm/write_seal/candidates",
        "core/parallel_vm/write_seal/matches",
        "core/parallel_vm/write_seal/mismatches",
        "core/parallel_vm/publish_audit/candidates",
        "core/parallel_vm/publish_audit/matches",
        "core/parallel_vm/publish_audit/mismatches",
        "core/parallel_vm/publish_audit/errors",
        "core/parallel_vm/errors",
        "core/parallel_vm/retry/async_publish/errors",
        "core/speculative_execution/safety_persist_errors",
        "core/parallel_transfer/published",
        "core/parallel_vm/published",
        "core/parallel_vm/block_energy/published",
    ]
    metrics = {name: {"count": 0} for name in names}
    metrics["core/speculative_execution/safety_disabled"] = {"value": 0}
    metrics["core/speculative_execution/safety_persisted"] = {"value": 0}
    metrics["core/parallel_transfer/enabled"] = {"value": 1}
    metrics["core/parallel_vm/enabled"] = {"value": 0}
    metrics["core/parallel_transfer/balance_oracle/candidates"]["count"] = 10
    metrics["core/parallel_transfer/balance_oracle/matches"]["count"] = 8
    metrics["core/parallel_transfer/balance_oracle/fallbacks"]["count"] = 2
    metrics["core/parallel_transfer/serial_verify/candidates"]["count"] = 8
    metrics["core/parallel_transfer/serial_verify/matches"]["count"] = 8
    metrics["core/parallel_transfer/write_seal/candidates"]["count"] = 8
    metrics["core/parallel_transfer/write_seal/matches"]["count"] = 8
    metrics["core/parallel_transfer/publish_audit/candidates"]["count"] = 8
    metrics["core/parallel_transfer/publish_audit/matches"]["count"] = 8
    metrics["core/parallel_transfer/published"]["count"] = 8
    return metrics


class ParallelExecutionReleaseGateTest(unittest.TestCase):
    def test_accepts_closed_accounting_and_zero_failures(self):
        self.assertEqual([], GATE.audit_metrics(clean_metrics(), require_transfer_publications=True))

    def test_rejects_safety_circuit_and_mismatch(self):
        metrics = clean_metrics()
        metrics["core/speculative_execution/safety_disabled"]["value"] = 1
        metrics["core/parallel_transfer/publish_audit/mismatches"]["count"] = 1
        issues = GATE.audit_metrics(metrics)
        self.assertTrue(any("safety_disabled=1" in issue for issue in issues))
        self.assertTrue(any("publish_audit/mismatches=1" in issue for issue in issues))

    def test_rejects_legacy_state_repair_activation(self):
        metrics = clean_metrics()
        metrics["core/mainnet_state_repair/parallel_vm_missed_payment"]["count"] = 1
        issues = GATE.audit_metrics(metrics)
        self.assertTrue(any("parallel_vm_missed_payment=1" in issue for issue in issues))

    def test_rejects_persisted_safety_incident_after_restart(self):
        metrics = clean_metrics()
        metrics["core/speculative_execution/safety_persisted"]["value"] = 1
        issues = GATE.audit_metrics(metrics)
        self.assertTrue(any("safety_persisted=1" in issue for issue in issues))

    def test_rejects_safety_marker_persistence_error(self):
        metrics = clean_metrics()
        metrics["core/speculative_execution/safety_persist_errors"]["count"] = 1
        issues = GATE.audit_metrics(metrics)
        self.assertTrue(any("safety_persist_errors=1" in issue for issue in issues))

    def test_accepts_diagnostic_serial_oracle_read_set_difference(self):
        metrics = clean_metrics()
        metrics["core/parallel_transfer/serial_verify/read_set_differences"]["count"] = 1
        issues = GATE.audit_metrics(metrics)
        self.assertEqual([], issues)

    def test_rejects_canonical_oracle_restore_mismatch(self):
        metrics = clean_metrics()
        metrics["core/parallel_vm/serial_verify/restore_mismatches"]["count"] = 1
        issues = GATE.audit_metrics(metrics)
        self.assertTrue(any("restore_mismatches=1" in issue for issue in issues))

    def test_rejects_unclosed_balance_oracle_accounting(self):
        metrics = clean_metrics()
        metrics["core/parallel_transfer/balance_oracle/matches"]["count"] = 7
        issues = GATE.audit_metrics(metrics)
        self.assertTrue(any("accounting does not close" in issue for issue in issues))

    def test_rejects_missing_metric_and_missing_vm_activity(self):
        metrics = clean_metrics()
        del metrics["core/parallel_vm/publish_audit/errors"]
        issues = GATE.audit_metrics(metrics, require_vm_publications=True)
        self.assertTrue(any("missing metric" in issue for issue in issues))
        self.assertTrue(any("VM gate has insufficient activity" in issue for issue in issues))

    def test_rejects_publication_without_one_for_one_post_apply_audit(self):
        metrics = clean_metrics()
        metrics["core/parallel_transfer/published"]["count"] = 9
        issues = GATE.audit_metrics(metrics)
        self.assertTrue(any("not one-for-one serial-verified" in issue for issue in issues))
        self.assertTrue(any("not one-for-one audited" in issue for issue in issues))

    def test_rejects_vm_publication_without_one_for_one_serial_verification(self):
        metrics = clean_metrics()
        metrics["core/parallel_vm/enabled"]["value"] = 1
        metrics["core/parallel_vm/published"]["count"] = 1
        metrics["core/parallel_vm/block_energy/published"]["count"] = 1
        metrics["core/parallel_vm/publish_audit/candidates"]["count"] = 1
        metrics["core/parallel_vm/publish_audit/matches"]["count"] = 1
        issues = GATE.audit_metrics(metrics)
        self.assertTrue(any("VM publications are not one-for-one serial-verified" in issue for issue in issues))

    def test_rejects_publications_missing_family_specific_safety_proof(self):
        metrics = clean_metrics()
        metrics["core/parallel_transfer/balance_oracle/matches"]["count"] = 7
        metrics["core/parallel_transfer/balance_oracle/fallbacks"]["count"] = 3
        metrics["core/parallel_vm/enabled"]["value"] = 1
        metrics["core/parallel_vm/published"]["count"] = 1
        metrics["core/parallel_vm/serial_verify/candidates"]["count"] = 1
        metrics["core/parallel_vm/serial_verify/matches"]["count"] = 1
        metrics["core/parallel_vm/dual_oracle/candidates"]["count"] = 1
        metrics["core/parallel_vm/dual_oracle/matches"]["count"] = 1
        metrics["core/parallel_vm/write_seal/candidates"]["count"] = 1
        metrics["core/parallel_vm/write_seal/matches"]["count"] = 1
        metrics["core/parallel_vm/publish_audit/candidates"]["count"] = 1
        metrics["core/parallel_vm/publish_audit/matches"]["count"] = 1
        issues = GATE.audit_metrics(metrics)
        self.assertTrue(any("not one-for-one balance-verified" in issue for issue in issues))
        self.assertTrue(any("not one-for-one block-energy settled" in issue for issue in issues))

    def test_rejects_vm_publication_without_dual_oracle_proof(self):
        metrics = clean_metrics()
        metrics["core/parallel_vm/enabled"]["value"] = 1
        metrics["core/parallel_vm/published"]["count"] = 1
        metrics["core/parallel_vm/serial_verify/candidates"]["count"] = 1
        metrics["core/parallel_vm/serial_verify/matches"]["count"] = 1
        metrics["core/parallel_vm/write_seal/candidates"]["count"] = 1
        metrics["core/parallel_vm/write_seal/matches"]["count"] = 1
        metrics["core/parallel_vm/publish_audit/candidates"]["count"] = 1
        metrics["core/parallel_vm/publish_audit/matches"]["count"] = 1
        metrics["core/parallel_vm/block_energy/published"]["count"] = 1
        issues = GATE.audit_metrics(metrics)
        self.assertTrue(any("not one-for-one dual-oracle verified" in issue for issue in issues))

        metrics["core/parallel_vm/dual_oracle/candidates"]["count"] = 1
        metrics["core/parallel_vm/dual_oracle/info_mismatches"]["count"] = 1
        issues = GATE.audit_metrics(metrics)
        self.assertTrue(any("dual_oracle/info_mismatches=1" in issue for issue in issues))

    def test_rejects_write_seal_mutation_and_missing_one_for_one_seal(self):
        metrics = clean_metrics()
        metrics["core/parallel_transfer/write_seal/matches"]["count"] = 7
        metrics["core/parallel_transfer/write_seal/mismatches"]["count"] = 1
        issues = GATE.audit_metrics(metrics)
        self.assertTrue(any("write_seal/mismatches=1" in issue for issue in issues))
        self.assertTrue(any("not one-for-one WriteSet-sealed" in issue for issue in issues))

    def test_requires_enabled_publisher_and_minimum_exposure(self):
        metrics = clean_metrics()
        metrics["core/parallel_transfer/enabled"]["value"] = 0
        issues = GATE.audit_metrics(metrics, min_transfer_publications=10)
        self.assertTrue(any("enabled=0" in issue for issue in issues))
        self.assertTrue(any("published=8, want >= 10" in issue for issue in issues))


if __name__ == "__main__":
    unittest.main()
