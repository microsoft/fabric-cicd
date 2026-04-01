# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Pre-built fallback using Tabular Editor 2/3 CLI for semantic model deployment."""

import logging
import shutil
import subprocess

logger = logging.getLogger(__name__)


def te2_deploy_fallback(
    workspace_name: str,
    item_name: str,
    item_path: str,
) -> None:
    """
    Deploy a semantic model via Tabular Editor CLI when the REST API
    fails with PurgeRequired.

    Requires: Tabular Editor 2 or 3 CLI on the system PATH.
    No additional Python dependencies needed.

    Args:
        workspace_name: The Fabric workspace display name (used for XMLA endpoint).
        item_name: Display name of the semantic model.
        item_path: Local path to the TMDL definition folder.

    Raises:
        FileNotFoundError: If TabularEditor executable is not found on PATH.
        subprocess.CalledProcessError: If the TE2 deployment command fails.
    """
    te_exe = _find_te_executable()

    # XMLA endpoint requires workspace name, not ID
    connection_string = f"powerbi://api.powerbi.com/v1.0/myorg/{workspace_name}"

    cmd = [
        te_exe,
        item_path,  # Source TMDL folder
        "-D",
        connection_string,  # Destination server
        item_name,  # Target database name
        "-O",  # Allow overwrite of existing model
        "-C",  # Deploy connection strings
        "-P",  # Deploy partition definitions
        "-R",  # Deploy role members
    ]

    logger.info(f"Deploying '{item_name}' via Tabular Editor")
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        if result.stdout:
            logger.debug(f"Tabular Editor output: {result.stdout}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Tabular Editor deployment failed for '{item_name}'")
        if e.stderr:
            logger.error(f"Tabular Editor stderr: {e.stderr}")
        if e.stdout:
            logger.error(f"Tabular Editor stdout: {e.stdout}")
        raise

    logger.info(f"Tabular Editor deployment succeeded for '{item_name}'")


def _find_te_executable() -> str:
    """Locate Tabular Editor executable on PATH."""
    for name in ["TabularEditor", "TabularEditor.exe", "te", "te.exe"]:
        path = shutil.which(name)
        if path:
            return path
    msg = (
        "Tabular Editor executable not found on PATH. "
        "Install TE2/TE3 and ensure it's available as 'TabularEditor' or 'te'. "
        "See: https://tabulareditor.com"
    )
    raise FileNotFoundError(msg)
