"""MSIT TEST workspace definition.

Inherits the full item set (lakehouses, access lists, spark env, SJDs, pipelines)
from ``msit_dev`` and overrides only the env-specific fields (target,
workspace_id, workspace_name). This guarantees TEST always carries the same
items as DEV — adding a pipeline/SJD/lakehouse in ``msit_dev.py`` automatically
applies here on the next deploy.
"""

from dataclasses import replace

from ._schema import TargetEnvironment
from .msit_dev import WORKSPACE as DEV_WORKSPACE

WORKSPACE = replace(
    DEV_WORKSPACE,
    target=TargetEnvironment.TEST,
    workspace_name="",  # resolve to "fabric-cicd-test" (or personal via FABRICCICD_USER)
    workspace_id="",  # filled in by `python fabriccicd.py create TEST`
)
