"""One-shot: create demo CSV files under Files/raw/ in Lakehouse1 and Lakehouse2 of DEV.

Creates a Fabric Notebook that writes 4 CSVs (mirroring the seeded tables) using
mssparkutils, runs it on demand, polls until complete, then exits.

Run:
    python seed_files.py
"""

import base64
import json
import sys
import time
import uuid
from pathlib import Path

import requests
from azure.identity import AzureCliCredential

sys.path.insert(0, str(Path(__file__).parent))
from fabriccicd_inputs import get_workspace  # noqa: E402

NOTEBOOK_NAME = "seed_demo_files"
BASE = "https://api.fabric.microsoft.com/v1"


def log(msg: str) -> None:
    print(f">> {msg}")


def get_token() -> str:
    return AzureCliCredential().get_token("https://api.fabric.microsoft.com/.default").token


def find_lakehouse(headers: dict, ws_id: str, name: str) -> dict:
    resp = requests.get(f"{BASE}/workspaces/{ws_id}/lakehouses", headers=headers)
    resp.raise_for_status()
    for lh in resp.json().get("value", []):
        if lh["displayName"] == name:
            return lh
    msg = f"Lakehouse '{name}' not found in workspace {ws_id}"
    raise RuntimeError(msg)


def build_notebook_ipynb(ws_id: str, lh1: dict, lh2: dict) -> dict:
    """Return a Jupyter notebook dict that writes 4 demo CSVs under Files/raw/."""
    code = f"""# Seed demo CSV files in Lakehouse1 and Lakehouse2 (Files/raw/)
import notebookutils

LH1_FILES = "abfss://{ws_id}@onelake.dfs.fabric.microsoft.com/{lh1["id"]}/Files"
LH2_FILES = "abfss://{ws_id}@onelake.dfs.fabric.microsoft.com/{lh2["id"]}/Files"

customers_csv = (
    "id,name,email,signup_date\\n"
    "1,Alice,alice@example.com,2026-01-15\\n"
    "2,Bob,bob@example.com,2026-02-03\\n"
    "3,Carol,carol@example.com,2026-02-21\\n"
    "4,Dave,dave@example.com,2026-03-10\\n"
    "5,Eve,eve@example.com,2026-04-01\\n"
)
orders_csv = (
    "order_id,customer_id,total,order_date\\n"
    "1001,1,49.99,2026-04-10\\n"
    "1002,2,129.50,2026-04-12\\n"
    "1003,1,19.99,2026-04-15\\n"
    "1004,3,249.00,2026-04-20\\n"
    "1005,4,75.25,2026-04-25\\n"
)
products_csv = (
    "sku,name,price,category\\n"
    "SKU-001,Widget,19.99,Tools\\n"
    "SKU-002,Gadget,49.99,Tools\\n"
    "SKU-003,Notebook,9.99,Stationery\\n"
    "SKU-004,Pen,1.99,Stationery\\n"
    "SKU-005,Backpack,79.99,Accessories\\n"
)
inventory_csv = (
    "sku,warehouse,quantity\\n"
    "SKU-001,WH-EAST,120\\n"
    "SKU-002,WH-EAST,50\\n"
    "SKU-003,WH-WEST,500\\n"
    "SKU-004,WH-WEST,1000\\n"
    "SKU-005,WH-EAST,75\\n"
)

notebookutils.fs.put(f"{{LH1_FILES}}/raw/customers.csv", customers_csv, True)
notebookutils.fs.put(f"{{LH1_FILES}}/raw/orders.csv", orders_csv, True)
notebookutils.fs.put(f"{{LH2_FILES}}/raw/products.csv", products_csv, True)
notebookutils.fs.put(f"{{LH2_FILES}}/raw/inventory.csv", inventory_csv, True)

print("Seeded customers.csv, orders.csv, products.csv, inventory.csv under Files/raw/.")
"""
    return {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "kernelspec": {"display_name": "Synapse PySpark", "name": "synapse_pyspark", "language": "Python"},
            "language_info": {"name": "python"},
            "microsoft": {"language": "python"},
            "trident": {
                "lakehouse": {
                    "default_lakehouse": lh1["id"],
                    "default_lakehouse_name": lh1["displayName"],
                    "default_lakehouse_workspace_id": ws_id,
                    "known_lakehouses": [{"id": lh1["id"]}, {"id": lh2["id"]}],
                }
            },
        },
        "cells": [
            {
                "cell_type": "code",
                "source": code.splitlines(keepends=True),
                "metadata": {},
                "outputs": [],
                "execution_count": None,
                "id": str(uuid.uuid4()),
            }
        ],
    }


def b64(text: str) -> str:
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


def create_or_get_notebook(headers: dict, ws_id: str, name: str, nb_dict: dict) -> str:
    resp = requests.get(f"{BASE}/workspaces/{ws_id}/notebooks", headers=headers)
    resp.raise_for_status()
    for nb in resp.json().get("value", []):
        if nb["displayName"] == name:
            log(f"Notebook '{name}' already exists (id={nb['id']}). Updating definition.")
            update_url = f"{BASE}/workspaces/{ws_id}/notebooks/{nb['id']}/updateDefinition"
            payload = {
                "definition": {
                    "format": "ipynb",
                    "parts": [
                        {
                            "path": "notebook-content.ipynb",
                            "payload": b64(json.dumps(nb_dict)),
                            "payloadType": "InlineBase64",
                        }
                    ],
                }
            }
            r = requests.post(update_url, headers=headers, json=payload)
            if r.status_code in (200, 202):
                return nb["id"]
            log(f"Update failed ({r.status_code}); reusing existing notebook anyway: {r.text}")
            return nb["id"]

    log(f"Creating notebook '{name}'...")
    payload = {
        "displayName": name,
        "definition": {
            "format": "ipynb",
            "parts": [
                {"path": "notebook-content.ipynb", "payload": b64(json.dumps(nb_dict)), "payloadType": "InlineBase64"}
            ],
        },
    }
    resp = requests.post(f"{BASE}/workspaces/{ws_id}/notebooks", headers=headers, json=payload)
    if resp.status_code == 201:
        return resp.json()["id"]
    if resp.status_code == 202:
        op_url = resp.headers["Location"]
        while True:
            time.sleep(5)
            poll = requests.get(op_url, headers=headers).json()
            if poll.get("status") == "Succeeded":
                result = requests.get(op_url + "/result", headers=headers)
                if result.status_code == 200 and result.text:
                    return result.json()["id"]
                resp2 = requests.get(f"{BASE}/workspaces/{ws_id}/notebooks", headers=headers)
                for nb in resp2.json().get("value", []):
                    if nb["displayName"] == name:
                        return nb["id"]
                msg = "Notebook created but ID not found"
                raise RuntimeError(msg)
            if poll.get("status") in ("Failed", "Cancelled"):
                msg = f"Notebook creation failed: {poll}"
                raise RuntimeError(msg)
    msg = f"Notebook creation failed: {resp.status_code} - {resp.text}"
    raise RuntimeError(msg)


def run_notebook(headers: dict, ws_id: str, notebook_id: str) -> None:
    log("Triggering notebook run...")
    url = f"{BASE}/workspaces/{ws_id}/items/{notebook_id}/jobs/instances?jobType=RunNotebook"
    resp = requests.post(url, headers=headers)
    if resp.status_code not in (200, 202):
        msg = f"Run failed: {resp.status_code} - {resp.text}"
        raise RuntimeError(msg)
    op_url = resp.headers.get("Location")
    if not op_url:
        log("Run started, no Location header to poll. Check workspace UI.")
        return

    log(f"Polling: {op_url}")
    while True:
        time.sleep(15)
        poll = requests.get(op_url, headers=headers)
        if poll.status_code != 200:
            log(f"Poll error: {poll.status_code} - {poll.text}")
            return
        body = poll.json()
        status = body.get("status", "")
        log(f"  Status: {status}")
        if status == "Completed":
            log("Notebook run completed successfully.")
            return
        if status in ("Failed", "Cancelled", "Deduped"):
            log(f"Notebook run ended: {status}")
            log(f"Details: {body}")
            return


def run(env: str = "DEV", realm_mode: bool = False) -> None:
    env = env.upper()
    ws = get_workspace(env, realm_mode=realm_mode)
    if not ws.workspace_id:
        log(f"{env} workspace_id is empty. Run 'python fabriccicd.py create {env}' first.")
        sys.exit(1)
    ws_id = ws.workspace_id
    log(f"Workspace: {ws_id}")

    headers = {"Authorization": f"Bearer {get_token()}", "Content-Type": "application/json"}

    lh1 = find_lakehouse(headers, ws_id, "Lakehouse1")
    lh2 = find_lakehouse(headers, ws_id, "Lakehouse2")
    log(f"Lakehouse1: {lh1['id']}")
    log(f"Lakehouse2: {lh2['id']}")

    nb_dict = build_notebook_ipynb(ws_id, lh1, lh2)
    notebook_id = create_or_get_notebook(headers, ws_id, NOTEBOOK_NAME, nb_dict)
    log(f"Notebook ID: {notebook_id}")

    run_notebook(headers, ws_id, notebook_id)


def main() -> None:
    env = (sys.argv[1] if len(sys.argv) > 1 else "DEV").upper()
    run(env)


if __name__ == "__main__":
    main()
