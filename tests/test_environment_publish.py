from pathlib import Path

from fabric_cicd._items import _environment as env_module


class DummyFile:
    def __init__(self, file_path):
        # accept Path or string
        self.file_path = Path(file_path)
        # keep attributes other code may inspect
        self.relative_path = str(self.file_path).replace("\\", "/")
        self.type = "text"
        self.base64_payload = "payload"
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


def test_set_env_setting_only(tmp_path):
    # Only Setting segment present
    p = tmp_path / "EnvA" / "Setting" / "Sparkcompute.yml"
    item = DummyItem("EnvA", [p])
    assert env_module.set_environment_deployment_type(item) is True


def test_set_env_with_publiclibraries(tmp_path):
    # Both Setting and Libraries present -> not shell-only
    p1 = tmp_path / "EnvB" / "Setting" / "Sparkcompute.yml"
    p2 = tmp_path / "EnvB" / "Libraries" / "PublicLibraries" / "lib.zip"
    item = DummyItem("EnvB", [p1, p2])
    assert env_module.set_environment_deployment_type(item) is False


def test_set_env_with_customlibraries(tmp_path):
    # CustomLibraries lives inside Libraries (standardized) -> counts as Libraries
    p1 = tmp_path / "EnvC" / "Setting" / "Sparkcompute.yml"
    p2 = tmp_path / "EnvC" / "Libraries" / "CustomLibraries" / "x.jar"
    item = DummyItem("EnvC", [p1, p2])
    assert env_module.set_environment_deployment_type(item) is False


def test_set_env_with_both_libraries(tmp_path):
    # Environment contains Setting plus both Public and Custom libraries under Libraries
    p1 = tmp_path / "EnvD" / "Setting" / "Sparkcompute.yml"
    p2 = tmp_path / "EnvD" / "Libraries" / "PublicLibraries" / "lib.zip"
    p3 = tmp_path / "EnvD" / "Libraries" / "CustomLibraries" / "custom.jar"
    item = DummyItem("EnvD", [p1, p2, p3])
    assert env_module.set_environment_deployment_type(item) is False


def test_set_env_with_empty_libraries_is_shell_only(tmp_path):
    # Libraries folder exists but contains no files -> should be treated as shell-only
    env_dir = tmp_path / "EnvE"
    (env_dir / "Setting").mkdir(parents=True, exist_ok=True)
    (env_dir / "Libraries").mkdir(parents=True, exist_ok=True)  # empty libraries folder

    p_setting = env_dir / "Setting" / "Sparkcompute.yml"
    # We don't need to actually write Sparkcompute.yml for this unit test, but
    # create the path to reflect the layout and pass it to DummyItem
    item = DummyItem("EnvE", [p_setting])

    assert env_module.set_environment_deployment_type(item) is True


def test_publish_environments_passes_shell_flag(tmp_path):
    """
    Ensure publish_environments computes shell-only and passes it to _publish_item.
    We provide a fake workspace where _publish_item is intercepted and marks the
    item as skipped so metadata publish is not attempted.
    """
    captured = {}

    class FakeEndpoint:
        def invoke(self, *_args, **_kwargs):
            # used by check_environment_publish_state and other calls
            return {"body": {"value": []}}

    class FakeWorkspace:
        def __init__(self):
            # one environment with Setting only
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
            # capture kwargs and mark item as skip_publish to stop metadata publishing
            captured["called_with"] = kwargs
            self.repository_items[item_type][item_name].skip_publish = True

    ws = FakeWorkspace()
    env_module.publish_environments(ws)
    assert "shell_only_publish" in captured["called_with"]
    assert captured["called_with"]["shell_only_publish"] is True


def test_convert_spark_conf_to_spark_properties():
    inp = {"spark_conf": {"a": "1", "b": "2"}, "some_key": {"inner_key": "value"}}
    out = env_module._convert_environment_compute_to_camel(None, inp)
    assert "sparkProperties" in out
    assert isinstance(out["sparkProperties"], list)
    kvs = {p["key"]: p["value"] for p in out["sparkProperties"]}
    assert kvs == {"a": "1", "b": "2"}


# ---------- End-to-end style tests below ----------


def test_end_to_end_environment_setting_only(tmp_path):
    """
    End-to-end style test for an Environment item that contains only Setting.
    Verifies: create item (POST /items), update compute (PATCH sparkcompute), and
    submit publish (POST /staging/publish).
    """
    # Prepare workspace structure with a real Sparkcompute.yml file
    env_dir = tmp_path / "EnvEnd1"
    setting_dir = env_dir / "Setting"
    setting_dir.mkdir(parents=True, exist_ok=True)
    spark_yaml = setting_dir / "Sparkcompute.yml"
    spark_yaml.write_text("instance_pool_id: demo_pool\n", encoding="utf-8")

    # capture endpoint calls
    calls = []

    class FakeEndpoint:
        def invoke(self, method=None, url=None, body=None, **_kwargs):
            calls.append((method, url, body))
            # Respond as the real endpoints would for these calls
            if method == "GET" and url.endswith("/environments/"):
                return {"body": {"value": []}}
            if method == "POST" and url.endswith("/items"):
                return {"body": {"id": "guid-123"}}
            if method == "PATCH" and "sparkcompute" in url:
                return {"status": 200}
            if method == "POST" and url.endswith("/staging/publish?beta=False"):
                return {"status": 202}
            # fallback
            return {}

    class FakeWorkspace:
        def __init__(self):
            p = spark_yaml
            self.repository_items = {"Environment": {"EnvEnd1": DummyItem("EnvEnd1", [p])}}
            self.publish_item_name_exclude_regex = None
            self.publish_folder_path_exclude_regex = None
            self.items_to_include = None
            self.base_api_url = "https://api.example"
            self.endpoint = FakeEndpoint()
            self.repository_directory = tmp_path
            self.responses = None
            # environment_parameter may be used by _update_compute_settings; keep empty
            self.environment_parameter = {}

        def _publish_item(self, item_name, item_type, **kwargs):
            # Simulate the create/update logic sufficiently for this test
            item = self.repository_items[item_type][item_name]
            kwargs.pop("shell_only_publish", None)
            # If item not deployed yet, simulate creation
            if not item.guid:
                resp = self.endpoint.invoke(method="POST", url=f"{self.base_api_url}/items", body={})
                item.guid = resp["body"]["id"]
            else:
                # simulate update path (not needed for this test)
                self.endpoint.invoke(
                    method="POST",
                    url=f"{self.base_api_url}/items/{item.guid}/updateDefinition?updateMetadata=True",
                    body={},
                )
            # leave skip_publish False so metadata publishing runs

    ws = FakeWorkspace()
    env_module.publish_environments(ws)

    # Verify the sequence of important calls occurred in order
    urls = [c[1] for c in calls]
    assert any("/items" in u and u.endswith("/items") for u in urls), "Create item call missing"
    assert any("staging/sparkcompute" in u for u in urls), "sparkcompute PATCH missing"
    assert any(u.endswith("/staging/publish?beta=False") for u in urls), "Publish submit missing"


def test_end_to_end_environment_with_libraries(tmp_path):
    """
    End-to-end style test for an Environment item that contains both Setting and
    Libraries. Verifies that the flow performs create/update and still submits
    publish and compute update.
    """
    env_dir = tmp_path / "EnvEnd2"
    setting_dir = env_dir / "Setting"
    libs_dir = env_dir / "Libraries"
    setting_dir.mkdir(parents=True, exist_ok=True)
    libs_dir.mkdir(parents=True, exist_ok=True)
    spark_yaml = setting_dir / "Sparkcompute.yml"
    spark_yaml.write_text("instance_pool_id: demo_pool\n", encoding="utf-8")
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
            if method == "PATCH" and "sparkcompute" in url:
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

        def _publish_item(self, item_name, item_type, **kwargs):
            item = self.repository_items[item_type][item_name]
            kwargs.pop("shell_only_publish", None)
            # For non-shell we simulate creation then update definition path
            if not item.guid:
                resp = self.endpoint.invoke(method="POST", url=f"{self.base_api_url}/items", body={})
                item.guid = resp["body"]["id"]
            # simulate updateDefinition
            self.endpoint.invoke(
                method="POST",
                url=f"{self.base_api_url}/items/{item.guid}/updateDefinition?updateMetadata=True",
                body={},
            )

    ws = FakeWorkspace()
    env_module.publish_environments(ws)

    urls = [c[1] for c in calls]
    assert any("/items" in u and u.endswith("/items") for u in urls), "Create item call missing"
    assert any("updateDefinition" in u for u in urls), "updateDefinition call missing"
    assert any("staging/sparkcompute" in u for u in urls), "sparkcompute PATCH missing"
    assert any(u.endswith("/staging/publish?beta=False") for u in urls), "Publish submit missing"


def test_update_compute_settings_replaces_instance_pool(tmp_path):
    # create Sparkcompute.yml with instance_pool_id
    env_dir = tmp_path / "EnvPool"
    (env_dir / "Setting").mkdir(parents=True)
    sc = env_dir / "Setting" / "Sparkcompute.yml"
    sc.write_text("instance_pool_id: demo_pool\n", encoding="utf-8")

    class FakeEndpoint:
        def invoke(self, *_args, **_kwargs):
            # return a benign response for PATCH
            return {"status": 200}

    class FakeWS:
        def __init__(self):
            self.environment = "DEV"
            self.environment_parameter = {
                "spark_pool": [{"instance_pool_id": "demo_pool", "replace_value": {"DEV": "resolved_pool"}}]
            }
            self.endpoint = FakeEndpoint()
            self.base_api_url = "https://api.example"

    from fabric_cicd._items._environment import _update_compute_settings

    # call _update_compute_settings with FakeWS, path, guid, name and assert it runs without error
    ws_instance = FakeWS()
    _update_compute_settings(ws_instance, env_dir, "guid", "EnvPool")
