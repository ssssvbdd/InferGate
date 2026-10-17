import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from capacity_report import load_events, report


class CapacityReportTests(unittest.TestCase):
    def test_dedup_cost_and_capacity(self):
        fixtures = Path(__file__).resolve().parent / "fixtures"
        events = load_events([fixtures / "capacity-one.jsonl", fixtures / "capacity-two.jsonl"])
        with self.subTest("two replicas"):
            result = report(events, {"qwen3-4b": {"input_per_million": 1.0,
                                                   "output_per_million": 2.0}},
                            safe_output_tps=1.0, target_load=0.5,
                            gpu_hours=2.0, gpu_hour_rate=3.0,
                            cpu_core_hours=8.0, cpu_core_hour_rate=0.1,
                            memory_gib_hours=16.0, memory_gib_hour_rate=0.02)
            self.assertEqual(result["request_count"], 2)
            self.assertEqual(result["peak_minute_output_tps"], 3.0)
            self.assertEqual(result["recommended_replicas_for_observed_peak"], 6)
            self.assertEqual(result["p95_ms"], 200)
            self.assertEqual(result["estimated_token_cost"], 0.00056)
            self.assertEqual(result["measured_gpu_hours_cost"], 6.0)
            self.assertEqual(result["measured_cpu_core_hours_cost"], 0.8)
            self.assertEqual(result["measured_memory_gib_hours_cost"], 0.32)

    def test_unmetered_event_not_billed(self):
        result = report([{"timestamp": "2026-10-16T10:00:00Z", "tenant": "a",
                          "model": "m", "status": 500, "metered": False}],
                        {"m": {"input_per_million": 1, "output_per_million": 1}})
        self.assertEqual(result["unmetered_requests"], 1)
        self.assertEqual(result["server_error_count"], 1)
        self.assertEqual(result["estimated_token_cost"], 0)

    def test_rejects_negative_tokens(self):
        with self.assertRaisesRegex(ValueError, "cannot be negative"):
            report([{"timestamp": "2026-10-16T10:00:00Z", "tenant": "a",
                     "model": "m", "metered": True, "prompt_tokens": -1}])

    def test_per_model_peak_does_not_mix_incompatible_replicas(self):
        events = [
            {"timestamp": "2026-10-16T10:00:00Z", "tenant": "a", "model": "small",
             "metered": True, "prompt_tokens": 10, "completion_tokens": 120},
            {"timestamp": "2026-10-16T10:00:00Z", "tenant": "b", "model": "large",
             "metered": True, "prompt_tokens": 10, "completion_tokens": 180},
        ]
        result = report(events, business_map={"a": "search", "b": "assistant"},
                        capacity_profiles={"small": {"safe_output_tps_per_replica": 4,
                                                      "target_load": 0.5},
                                           "large": {"safe_output_tps_per_replica": 2,
                                                     "target_load": 0.5}})
        by_model = {row["model"]: row for row in result["by_model_capacity"]}
        self.assertEqual(by_model["small"]["recommended_replicas_for_observed_peak"], 1)
        self.assertEqual(by_model["large"]["recommended_replicas_for_observed_peak"], 3)
        self.assertEqual({row["business"] for row in result["by_business"]},
                         {"search", "assistant"})


if __name__ == "__main__":
    unittest.main()
