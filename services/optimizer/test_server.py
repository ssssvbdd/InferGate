import unittest

from server import parse_gateway_metrics


class GatewayMetricsTests(unittest.TestCase):
    def test_parse_aggregates_stream_labels_without_prompt_content(self):
        parsed = parse_gateway_metrics('\n'.join([
            'infergate_workload_requests_total{class="balanced",stream="false"} 2',
            'infergate_workload_requests_total{class="balanced",stream="true"} 3',
            'infergate_estimated_prompt_tokens_total 80',
            'infergate_requested_output_tokens_total 64',
        ]))
        self.assertEqual(parsed.workloads, {"balanced": 5.0})
        self.assertEqual(parsed.estimated_prompt_tokens, 80.0)
        self.assertEqual(parsed.requested_output_tokens, 64.0)


if __name__ == "__main__":
    unittest.main()
