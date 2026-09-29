import json
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from optimizer import (OptimizationController, OptimizationPolicy, analyze_request,
                       parse_prometheus_signals)


class OptimizerTests(unittest.TestCase):
    def test_policy_rejects_actuation_mode(self):
        path = Path.cwd() / "run" / f"policy-test-{uuid.uuid4().hex}.json"
        path.parent.mkdir(exist_ok=True)
        try:
            path.write_text(json.dumps({"schema_version": 1, "mode": "active"}))
            with self.assertRaisesRegex(ValueError, "observe mode"):
                OptimizationPolicy.from_path(str(path))
        finally:
            path.unlink(missing_ok=True)

    def test_request_classification_does_not_store_content(self):
        policy = OptimizationPolicy(long_prompt_tokens=8, long_output_tokens=16)
        prefill = analyze_request(json.dumps({
            "messages": [{"role": "user", "content": "中文请求内容需要被估算"}],
            "max_tokens": 4,
        }, ensure_ascii=False).encode(), "/v1/chat/completions", 2, policy)
        decode = analyze_request(json.dumps({"prompt": "short", "max_tokens": 32}).encode(),
                                 "/v1/completions", 1, policy)
        self.assertEqual(prefill.workload_class, "prefill")
        self.assertEqual(decode.workload_class, "decode")
        self.assertNotIn("content", repr(prefill))

    def test_prometheus_signals_and_recommendation_hold(self):
        signals = parse_prometheus_signals(
            'vllm:kv_cache_usage_perc{model_name="qwen"} 0.91\n'
            'vllm:num_preemptions_total 4\n'
            'vllm:num_requests_waiting 3\n')
        self.assertEqual(signals.kv_cache_usage, 0.91)
        self.assertEqual(signals.preemptions_total, 4)
        controller = OptimizationController(OptimizationPolicy(
            recommendation_hold_seconds=30))
        with patch("optimizer.time.monotonic", return_value=100):
            controller.update_runtime("vllm:num_preemptions_total 4\n")
        with patch("optimizer.time.monotonic", return_value=101):
            controller.update_runtime("vllm:num_preemptions_total 5\n")
            self.assertEqual(controller.snapshot()["recommended_service_profile"],
                             "bf16-memory-090")
        with patch("optimizer.time.monotonic", return_value=132):
            self.assertEqual(controller.snapshot()["recommended_service_profile"],
                             "bf16-memory-085")


if __name__ == "__main__":
    unittest.main()
