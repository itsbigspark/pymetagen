mkdir -p .mypy-reports
mypy | tee .mypy-reports/report.txt || true