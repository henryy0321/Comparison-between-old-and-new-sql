import importlib.util
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "run_benchmark.py"
SPEC = importlib.util.spec_from_file_location("run_benchmark", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(MODULE)


class BenchmarkHelperTests(unittest.TestCase):
    def test_percentile_95(self):
        self.assertEqual(MODULE.percentile([1, 2, 3, 4, 5], 95), 5)

    def test_memory_to_mib(self):
        self.assertAlmostEqual(MODULE.parse_memory_mib("1.5GiB / 8GiB"), 1536.0)

    def test_generated_field_is_100_characters(self):
        value = MODULE.make_field_value(12, "field0")
        self.assertEqual(len(value), 100)

    def test_workload_ratios_add_to_one(self):
        for workload in MODULE.load_workloads().values():
            self.assertAlmostEqual(workload["read"] + workload["update"], 1.0)


if __name__ == "__main__":
    unittest.main()
