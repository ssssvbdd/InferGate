import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import threading
import unittest

import lab


class FakeOpenAIServer(BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def do_GET(self):
        if self.path == "/v1/models":
            body = json.dumps({"data": [{"id": "test-model"}]}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path == "/metrics":
            body = b"# HELP dummy\nvllm:prefix_cache_hits_total 2\n"
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_error(404)

    def do_POST(self):
        if self.path != "/v1/chat/completions":
            self.send_error(404)
            return
        size = int(self.headers["Content-Length"])
        payload = json.loads(self.rfile.read(size))
        assert payload["stream"] is True
        assert payload["model"] == "test-model"
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()
        for event in (
            {"choices": [{"delta": {"role": "assistant"}}]},
            {"choices": [{"delta": {"content": "OK"}}]},
            {"choices": [], "usage": {"prompt_tokens": 25, "completion_tokens": 2}},
        ):
            self.wfile.write(f"data: {json.dumps(event)}\n\n".encode())
            self.wfile.flush()
        self.wfile.write(b"data: [DONE]\n\n")


class LabTests(unittest.TestCase):
    scratch_root = Path(__file__).resolve().parent / "_scratch"

    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), FakeOpenAIServer)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.url = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=5)

    def test_workload_reuses_prefix_and_is_deterministic(self):
        first = lab.make_workload(42, 2, 3, 20, 2)
        self.assertEqual(first, lab.make_workload(42, 2, 3, 20, 2))
        cold, eviction, warm = first
        self.assertEqual((len(cold), len(eviction), len(warm)), (2, 2, 4))
        self.assertEqual(cold[0]["prompt"].split("Question:")[0],
                         warm[0]["prompt"].split("Question:")[0])
        self.assertNotEqual(cold[0]["prompt"].split("Question:")[0],
                            cold[1]["prompt"].split("Question:")[0])

    def test_end_to_end_client_and_report(self):
        self.scratch_root.mkdir(exist_ok=True)
        first = self.scratch_root / "baseline.json"
        second = self.scratch_root / "lmcache.json"
        common = ["--url", self.url, "--allow-unmanaged", "--prefixes", "2", "--suffixes", "2",
                  "--prefix-words", "20", "--evict-count", "1", "--max-tokens", "2"]
        self.assertEqual(lab.main(["run", "--mode", "baseline", "--output", str(first), *common]), 0)
        self.assertEqual(lab.main(["run", "--mode", "lmcache", "--output", str(second), *common]), 0)
        one = json.loads(first.read_text())
        two = json.loads(second.read_text())
        self.assertEqual(one["fingerprint"], two["fingerprint"])
        self.assertEqual(one["summaries"]["warm"]["success"], 2)
        self.assertEqual(one["summaries"]["warm"]["output_tokens"], 4)
        self.assertTrue(one["results"]["warm"][0]["output_sha256"])
        self.assertTrue(one["metrics_before"])
        report = self.scratch_root / "report.md"
        self.assertEqual(lab.main(["report", str(first), str(second), "--output", str(report)]), 0)
        self.assertIn("lmcache", report.read_text())

    def test_report_rejects_different_workloads(self):
        self.scratch_root.mkdir(exist_ok=True)
        a, b = self.scratch_root / "a.json", self.scratch_root / "b.json"
        a.write_text(json.dumps({"fingerprint": "one", "mode": "baseline"}))
        b.write_text(json.dumps({"fingerprint": "two", "mode": "prefix"}))
        self.assertEqual(lab.main(["report", str(a), str(b)]), 1)

    def test_run_rejects_wrong_server_mode(self):
        self.scratch_root.mkdir(exist_ok=True)
        meta = self.scratch_root / "active_server.json"
        meta.write_text(json.dumps({"mode": "prefix", "served_model": "test-model",
                                    "url": self.url, "gpu": {"index": 0}}))
        result = self.scratch_root / "wrong-mode.json"
        self.assertEqual(lab.main(["run", "--mode", "lmcache", "--url", self.url,
                                   "--server-meta", str(meta), "--output", str(result)]), 1)
        self.assertFalse(result.exists())

    def test_promote_creates_review_only_manifest(self):
        self.scratch_root.mkdir(exist_ok=True)
        baseline = self.scratch_root / "promote-baseline.json"
        candidate = self.scratch_root / "promote-candidate.json"
        output = self.scratch_root / "experiment-manifest.json"

        def run(mode, throughput, digest):
            return {
                "fingerprint": "same", "model": "test-model", "mode": mode,
                "server_meta": {"vllm_version": "test"},
                "summaries": {"warm": {"failed": 0, "ttft_p95_s": 0.1,
                                          "tpot_mean_s": 0.02,
                                          "output_tokens_per_s": throughput}},
                "results": {"warm": [{"id": "p0-s1", "ok": True,
                                          "output_sha256": digest}]},
            }

        baseline.write_text(json.dumps(run("baseline", 100, "abc")))
        candidate.write_text(json.dumps(run("prefix", 110, "abc")))
        self.assertEqual(lab.main(["promote", "--baseline", str(baseline),
                                   "--candidate", str(candidate), "--profile", "prefix-v1",
                                   "--output", str(output)]), 0)
        manifest = json.loads(output.read_text())
        self.assertEqual(manifest["status"], "eligible_for_review")
        self.assertFalse(manifest["auto_apply"])
        self.assertTrue(all(manifest["gates"].values()))


if __name__ == "__main__":
    unittest.main()
