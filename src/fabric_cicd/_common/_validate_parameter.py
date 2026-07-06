# import json
# from pathlib import Path

# import yaml
# from jsonpath_ng.ext import parse


# def validate_key_value_replace(
#     parameter_file_path: Path,
#     repository_directory: Path,
#     environment: str,
# ) -> dict:
#     """
#     Validate replacement rules efficiently.

#     Each JSON file is read only once.
#     """
#     with parameter_file_path.open(encoding="utf-8") as file:
#         config = yaml.safe_load(file)

#     rules = []

#     # Pre-compile JSONPath expressions
#     for idx, rule in enumerate(config.get("key_value_replace", [])):
#         rules.append({
#             "rule_id": idx,
#             "json_path": rule["find_key"],
#             "compiled_path": parse(rule["find_key"]),
#             "file_path": rule.get("file_path"),
#             "new_value": rule.get("replace_value", {}).get(environment),
#             "matches_found": 0,
#         })

#     results = []

#     all_json_files = list(repository_directory.rglob("*.json"))

#     for json_file in all_json_files:
#         try:
#             with json_file.open(encoding="utf-8") as file:
#                 data = json.load(file)

#         except Exception as e:
#             results.append({
#                 "file_path": str(json_file),
#                 "found": False,
#                 "error": f"Cannot load JSON: {e}",
#             })
#             continue

#         for rule in rules:
#             # Skip files not targeted by the rule
#             if rule["file_path"]:
#                 target = Path(rule["file_path"])

#                 try:
#                     if json_file.resolve() != target.resolve():
#                         continue
#                 except Exception:
#                     if str(json_file) != str(target):
#                         continue

#             matches = rule["compiled_path"].find(data)

#             for match in matches:
#                 rule["matches_found"] += 1

#                 results.append({
#                     "rule_id": rule["rule_id"],
#                     "json_path": rule["json_path"],
#                     "file_path": str(json_file),
#                     "match_location": str(match.full_path),
#                     "current_value": match.value,
#                     "new_value": rule["new_value"],
#                     "found": True,
#                 })

#     # Report rules with no matches
#     for rule in rules:
#         if rule["matches_found"] == 0:
#             results.append({
#                 "rule_id": rule["rule_id"],
#                 "json_path": rule["json_path"],
#                 "file_path": rule["file_path"],
#                 "found": False,
#                 "new_value": rule["new_value"],
#                 "error": "No matches found",
#             })

#     result_obj = {
#         "total_rules": len(rules),
#         "rules_without_matches": sum(1 for rule in rules if rule["matches_found"] == 0),
#         "total_matches": sum(rule["matches_found"] for rule in rules),
#         "matches": results,
#     }

#     return result_obj


# #  Inspiration to check metadata type. Need to implement later.
# # Need to implement for item name as well.
# def _validate_item_name(self, input_name: str) -> tuple[bool, str]:
#     """Validate the item name is found in the repository directory."""
#     item_name_list = []
#     for root, _dirs, files in os.walk(self.repository_directory):
#         directory = Path(root)
#         # valid item directory with .platform file within
#         if ".platform" in files:
#             item_metadata_path = Path(directory, ".platform")
#             with Path.open(item_metadata_path, encoding="utf-8") as file:
#                 item_metadata = json.load(file)
#             # Ensure required metadata fields are present
#             if item_metadata and "type" in item_metadata["metadata"] and "displayName" in item_metadata["metadata"]:
#                 item_name = item_metadata["metadata"]["displayName"]
#                 item_name_list.append(item_name)
