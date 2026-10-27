"""Serverless Migration Control Tower (SMCT).

Fleet-wide discovery, readiness scoring, automated remediation, A/B validation,
promotion/rollback and cost tracking for classic -> serverless migrations.
Core modules are dependency-free (stdlib only) so they are unit-testable offline;
Databricks SDK / Spark are imported lazily by the modules that need them.
"""
__version__ = "1.0.0"
