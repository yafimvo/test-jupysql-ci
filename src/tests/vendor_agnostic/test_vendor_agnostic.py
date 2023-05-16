import pytest
from vendor_agnostic.results_collector import ResultsCollector

IP = ["postgresql", "mssql", "sqlite", "duckdb", "mariadb", "snowflake", "questdb"]

BASE_DIR = "./src/tests/vendor_agnostic"


def test_ggplot(capsys):
    files = ["test_ggplot.py"]
    run(files, capsys)


def run(files, capsys):
    files = [f"{BASE_DIR}/{file}" for file in files]

    collector = ResultsCollector()
    for test_file in files:
        for ip in IP:
            if ip == "snowflake":
                args_ = ["--live", "-s", "--ip", ip, test_file]
            else:
                args_ = ["-s", "--ip", ip, test_file]

            pytest.main(plugins=[collector], args=args_)

            with capsys.disabled():
                print("\n\n============ Test results for {ip} ============")
                print("passed : ", collector.passed)
                print("failed : ", collector.failed)
                print("xfailed : ", collector.xfailed)
                print("skipped : ", collector.skipped)

        assert collector.failed == 0
