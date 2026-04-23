from pathlib import Path
from typing import ClassVar

import yaml

from fabric_cicd._items import _environment as env_module


class DummyFile:
    def __init__(self, file_path):
        # accept Path or string
        self.file_path = Path(file_path)
        # keep attributes other code may inspect
        self.relative_path = str(self.file_path).replace("\\", "/")
        self.type = "text"
        self.base64_payload = "payload"
        # Read contents from file if it exists, otherwise empty string
        if self.file_path.exists():
            self.contents = self.file_path.read_text(encoding="utf-8")
        else:
            self.contents = ""


class DummyItem:
    def __init__(self, name, file_paths):
        self.name = name
        self.item_files = [DummyFile(p) for p in file_paths]
        # Set path to the environment item directory (parent of the folder that
        # contains the file), e.g. if file is /tmp/Env/Setting/Sparkcompute.yml ->
        # path should be /tmp/Env
        if file_paths:
            p = Path(file_paths[0])
            # if parent has at least one parent, choose grandparent; otherwise use parent
            self.path = p.parent.parent if p.parent.parent != Path() else p.parent
        else:
            self.path = Path()
        self.guid = None
        self.skip_publish = False


# ---------- func_process_file tests ----------


def test_process_environment_file_non_sparkcompute(tmp_path):
    """Non-Sparkcompute files are returned unchanged."""
    f = tmp_path / "EnvA" / "Libraries" / "lib.txt"
    f.parent.mkdir(parents=True, exist_ok=True)
    f.write_text("original content", encoding="utf-8")

    dummy = DummyFile(f)
    result = env_module._process_environment_file(None, DummyItem("EnvA", [f]), dummy)
    assert result == "original content"


def test_process_environment_file_no_instance_pool(tmp_path):
    """Sparkcompute.yml without instance_pool_id is returned as re-serialized YAML."""
    env_dir = tmp_path / "EnvB"
    setting_dir = env_dir / "Setting"
    setting_dir.mkdir(parents=True, exist_ok=True)
    sc = setting_dir / "Sparkcompute.yml"
    sc.write_text("driver_cores: 8\ndriver_memory: 56g\n", encoding="utf-8")

    dummy = DummyFile(sc)
    result = env_module._process_environment_file(None, DummyItem("EnvB", [sc]), dummy)
    parsed = yaml.safe_load(result)
    assert parsed["driver_cores"] == 8
    assert parsed["driver_memory"] == "56g"
    assert "instance_pool_id" not in parsed


def test_process_environment_file_replaces_instance_pool(tmp_path):
    """Sparkcompute.yml with instance_pool_id is resolved to the target pool GUID via API."""
    env_dir = tmp_path / "EnvC"
    setting_dir = env_dir / "Setting"
    setting_dir.mkdir(parents=True, exist_ok=True)
    sc = setting_dir / "Sparkcompute.yml"
    sc.write_text("instance_pool_id: pool-123\ndriver_cores: 4\n", encoding="utf-8")

    class FakeEndpoint:
        def invoke(self, method=None, url=None, **_kwargs):
            if method == "GET" and url.endswith("/spark/pools"):
                return {
                    "body": {
                        "value": [
                            {"id": "resolved-guid-abc", "name": "MyPool", "type": "Capacity"},
                            {"id": "other-guid", "name": "OtherPool", "type": "Workspace"},
                        ]
                    }
                }
            return {}

    class FakeWS:
        environment = "DEV"
        environment_parameter: ClassVar[dict] = {
            "spark_pool": [
                {
                    "instance_pool_id": "pool-123",
                    "replace_value": {"DEV": {"type": "Capacity", "name": "MyPool"}},
                }
            ]
        }
        base_api_url = "https://api.example/v1/workspaces/ws-id"
        endpoint = FakeEndpoint()

    dummy = DummyFile(sc)
    result = env_module._process_environment_file(FakeWS(), DummyItem("EnvC", [sc]), dummy)
    parsed = yaml.safe_load(result)
    assert parsed["instance_pool_id"] == "resolved-guid-abc"
    assert "instance_pool" not in parsed
    assert parsed["driver_cores"] == 4


def test_process_environment_file_pool_with_item_name_filter(tmp_path):
    """instance_pool_id replacement respects the optional item_name filter."""
    env_dir = tmp_path / "EnvD"
    setting_dir = env_dir / "Setting"
    setting_dir.mkdir(parents=True, exist_ok=True)
    sc = setting_dir / "Sparkcompute.yml"
    sc.write_text("instance_pool_id: pool-456\n", encoding="utf-8")

    class FakeEndpoint:
        def invoke(self, method=None, url=None, **_kwargs):
            if method == "GET" and url.endswith("/spark/pools"):
                return {"body": {"value": [{"id": "ws-pool-guid", "name": "WsPool", "type": "Workspace"}]}}
            return {}

    class FakeWS:
        environment = "PROD"
        environment_parameter: ClassVar[dict] = {
            "spark_pool": [
                {
                    "instance_pool_id": "pool-456",
                    "replace_value": {"PROD": {"type": "Workspace", "name": "WsPool"}},
                    "item_name": "EnvD",
                }
            ]
        }
        base_api_url = "https://api.example/v1/workspaces/ws-id"
        endpoint = FakeEndpoint()

    dummy = DummyFile(sc)
    result = env_module._process_environment_file(FakeWS(), DummyItem("EnvD", [sc]), dummy)
    parsed = yaml.safe_load(result)
    assert parsed["instance_pool_id"] == "ws-pool-guid"


def test_process_environment_file_pool_no_match(tmp_path):
    """When no spark_pool entry matches, instance_pool_id is left as-is."""
    env_dir = tmp_path / "EnvE"
    setting_dir = env_dir / "Setting"
    setting_dir.mkdir(parents=True, exist_ok=True)
    sc = setting_dir / "Sparkcompute.yml"
    sc.write_text("instance_pool_id: unmatched-pool\n", encoding="utf-8")

    class FakeWS:
        environment = "DEV"
        environment_parameter: ClassVar[dict] = {
            "spark_pool": [{"instance_pool_id": "different-pool", "replace_value": {"DEV": "something"}}]
        }

    dummy = DummyFile(sc)
    result = env_module._process_environment_file(FakeWS(), DummyItem("EnvE", [sc]), dummy)
    parsed = yaml.safe_load(result)
    assert "instance_pool_id" in parsed
    assert parsed["instance_pool_id"] == "unmatched-pool"


def test_process_environment_file_no_spark_pool_param(tmp_path):
    """When environment_parameter has no spark_pool, instance_pool_id is left as-is."""
    env_dir = tmp_path / "EnvF"
    setting_dir = env_dir / "Setting"
    setting_dir.mkdir(parents=True, exist_ok=True)
    sc = setting_dir / "Sparkcompute.yml"
    sc.write_text("instance_pool_id: some-pool\n", encoding="utf-8")

    class FakeWS:
        environment = "DEV"
        environment_parameter: ClassVar[dict] = {}

    dummy = DummyFile(sc)
    result = env_module._process_environment_file(FakeWS(), DummyItem("EnvF", [sc]), dummy)
    parsed = yaml.safe_load(result)
    assert parsed["instance_pool_id"] == "some-pool"


def test_resolve_pool_id_success():
    """_resolve_pool_id returns the matching pool GUID from the API response."""

    class FakeEndpoint:
        def invoke(self, _method=None, _url=None, **_kwargs):
            return {
                "body": {
                    "value": [
                        {"id": "guid-cap", "name": "CapPool", "type": "Capacity"},
                        {"id": "guid-ws", "name": "WsPool", "type": "Workspace"},
                    ]
                }
            }

    class FakeWS:
        base_api_url = "https://api.example/v1/workspaces/ws-id"
        endpoint = FakeEndpoint()

    assert env_module._resolve_pool_id(FakeWS(), pool_name="CapPool", pool_type="Capacity") == "guid-cap"
    assert env_module._resolve_pool_id(FakeWS(), pool_name="WsPool", pool_type="Workspace") == "guid-ws"


def test_resolve_pool_id_not_found():
    """_resolve_pool_id raises when no matching pool is found."""
    import pytest

    class FakeEndpoint:
        def invoke(self, _method=None, _url=None, **_kwargs):
            return {"body": {"value": [{"id": "guid-other", "name": "OtherPool", "type": "Capacity"}]}}

    class FakeWS:
        base_api_url = "https://api.example/v1/workspaces/ws-id"
        endpoint = FakeEndpoint()

    with pytest.raises(Exception, match="Could not resolve custom Spark pool"):
        env_module._resolve_pool_id(FakeWS(), pool_name="MissingPool", pool_type="Workspace")


# ---------- Publisher integration tests ----------


def test_publish_environments_passes_func_process_file(tmp_path):
    """
    Ensure EnvironmentPublisher passes func_process_file to _publish_item
    and no longer passes shell_only_publish or exclude_path.
    """
    captured = {}

    class FakeEndpoint:
        def invoke(self, *_args, **_kwargs):
            return {"body": {"value": []}}

    class FakeWorkspace:
        def __init__(self):
            p = tmp_path / "EnvX" / "Setting" / "Sparkcompute.yml"
            self.repository_items = {"Environment": {"EnvX": DummyItem("EnvX", [p])}}
            self.publish_item_name_exclude_regex = None
            self.publish_folder_path_exclude_regex = None
            self.items_to_include = None
            self.base_api_url = "https://example"
            self.endpoint = FakeEndpoint()
            self.repository_directory = tmp_path
            self.responses = None

        def _publish_item(self, item_name, item_type, **kwargs):
            captured["called_with"] = kwargs
            self.repository_items[item_type][item_name].skip_publish = True

    ws = FakeWorkspace()
    env_module.EnvironmentPublisher(ws).publish_all()
    assert "func_process_file" in captured["called_with"]
    assert captured["called_with"]["func_process_file"] is env_module._process_environment_file
    assert "shell_only_publish" not in captured["called_with"]
    assert "exclude_path" not in captured["called_with"]


# ---------- End-to-end style tests ----------


def test_end_to_end_environment_setting_only(tmp_path):
    """
    End-to-end style test for an Environment item that contains only Setting.
    Verifies: create item (POST /items) and submit publish (POST /staging/publish).
    Sparkcompute.yml is included in the regular item definition—no separate
    PATCH sparkcompute call is made.
    """
    env_dir = tmp_path / "EnvEnd1"
    setting_dir = env_dir / "Setting"
    setting_dir.mkdir(parents=True, exist_ok=True)
    spark_yaml = setting_dir / "Sparkcompute.yml"
    spark_yaml.write_text("driver_cores: 4\n", encoding="utf-8")

    calls = []

    class FakeEndpoint:
        def invoke(self, method=None, url=None, body=None, **_kwargs):
            calls.append((method, url, body))
            if method == "GET" and url.endswith("/environments/"):
                return {"body": {"value": []}}
            if method == "POST" and url.endswith("/items"):
                return {"body": {"id": "guid-123"}}
            if method == "POST" and url.endswith("/staging/publish?beta=False"):
                return {"status": 202}
            return {}

    class FakeWorkspace:
        def __init__(self):
            self.repository_items = {"Environment": {"EnvEnd1": DummyItem("EnvEnd1", [spark_yaml])}}
            self.publish_item_name_exclude_regex = None
            self.publish_folder_path_exclude_regex = None
            self.items_to_include = None
            self.base_api_url = "https://api.example"
            self.endpoint = FakeEndpoint()
            self.repository_directory = tmp_path
            self.responses = None
            self.environment_parameter = {}

        def _publish_item(self, item_name, item_type, **_kwargs):
            item = self.repository_items[item_type][item_name]
            if not item.guid:
                resp = self.endpoint.invoke(method="POST", url=f"{self.base_api_url}/items", body={})
                item.guid = resp["body"]["id"]

    ws = FakeWorkspace()
    env_module.EnvironmentPublisher(ws).publish_all()

    urls = [c[1] for c in calls]
    assert any("/items" in u and u.endswith("/items") for u in urls), "Create item call missing"
    assert any(u.endswith("/staging/publish?beta=False") for u in urls), "Publish submit missing"
    assert not any("sparkcompute" in u for u in urls), "Unexpected sparkcompute PATCH call"


def test_end_to_end_environment_with_libraries(tmp_path):
    """
    End-to-end style test for an Environment item that contains both Setting and
    Libraries. Verifies create/update flow and staging publish—no separate
    PATCH sparkcompute call.
    """
    env_dir = tmp_path / "EnvEnd2"
    setting_dir = env_dir / "Setting"
    libs_dir = env_dir / "Libraries"
    setting_dir.mkdir(parents=True, exist_ok=True)
    libs_dir.mkdir(parents=True, exist_ok=True)
    spark_yaml = setting_dir / "Sparkcompute.yml"
    spark_yaml.write_text("driver_cores: 8\n", encoding="utf-8")
    (libs_dir / "lib.zip").write_text("dummy", encoding="utf-8")

    calls = []

    class FakeEndpoint:
        def invoke(self, method=None, url=None, body=None, **_kwargs):
            calls.append((method, url, body))
            if method == "GET" and url.endswith("/environments/"):
                return {"body": {"value": []}}
            if method == "POST" and url.endswith("/items"):
                return {"body": {"id": "guid-456"}}
            if method == "POST" and "updateDefinition" in url:
                return {"status": 200}
            if method == "POST" and url.endswith("/staging/publish?beta=False"):
                return {"status": 202}
            return {}

    class FakeWorkspace:
        def __init__(self):
            p_set = spark_yaml
            p_lib = libs_dir / "lib.zip"
            self.repository_items = {"Environment": {"EnvEnd2": DummyItem("EnvEnd2", [p_set, p_lib])}}
            self.publish_item_name_exclude_regex = None
            self.publish_folder_path_exclude_regex = None
            self.items_to_include = None
            self.base_api_url = "https://api.example"
            self.endpoint = FakeEndpoint()
            self.repository_directory = tmp_path
            self.responses = None
            self.environment_parameter = {}

        def _publish_item(self, item_name, item_type, **_kwargs):
            item = self.repository_items[item_type][item_name]
            if not item.guid:
                resp = self.endpoint.invoke(method="POST", url=f"{self.base_api_url}/items", body={})
                item.guid = resp["body"]["id"]
            self.endpoint.invoke(
                method="POST",
                url=f"{self.base_api_url}/items/{item.guid}/updateDefinition?updateMetadata=True",
                body={},
            )

    ws = FakeWorkspace()
    env_module.EnvironmentPublisher(ws).publish_all()

    urls = [c[1] for c in calls]
    assert any("/items" in u and u.endswith("/items") for u in urls), "Create item call missing"
    assert any("updateDefinition" in u for u in urls), "updateDefinition call missing"
    assert any(u.endswith("/staging/publish?beta=False") for u in urls), "Publish submit missing"
    assert not any("sparkcompute" in u for u in urls), "Unexpected sparkcompute PATCH call"
