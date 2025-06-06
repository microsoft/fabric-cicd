import re
from collections import defaultdict

# Sample M code input (from user message)
m_code = """
[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Dataflow Source_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"Dataflow Source" = let
  Source = PowerPlatform.Dataflows([]),
  #"Navigation 1" = Source{[Id = "Workspaces"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[workspaceId = "82a46da2-618c-4ca4-8d48-96d18da8d685"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[dataflowId = "5f3aae77-f860-4a23-9b75-5b15f888494b"]}[Data],
  #"Navigation 4" = #"Navigation 3"{[entity = "Mapping_ProductMaster", version = ""]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 4", each [CustomProduct] &lt;&gt; null and [CustomProduct] &lt;&gt; ""),
  #"Inserted conditional column" = Table.AddColumn(#"Filtered rows", "Is PBI", each if [CustomProduct] = "Power BI" then "Yes" else "No")
in
  #"Inserted conditional column";
shared #"Dataflow Source_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "82a46da2-618c-4ca4-8d48-96d18da8d687"]}[Data],
  Navigation_2 = Navigation_1{[dataflowId = "31dcc456-a6b6-46e2-824b-24f1e44b7297"]}[Data],
  TableNavigation = Navigation_2{[Id = "Destination", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
"""

# Compile regex patterns
workspace_pattern = re.compile(r'workspaceId\s*=\s*"([0-9a-fA-F\-]{36})"')
dataflow_pattern = re.compile(r'dataflowId\s*=\s*"([0-9a-fA-F\-]{36})"')

# Find all matches with positions
workspace_matches = [(m.start(), "workspaceId", m.group(1)) for m in workspace_pattern.finditer(m_code)]
dataflow_matches = [(m.start(), "dataflowId", m.group(1)) for m in dataflow_pattern.finditer(m_code)]
print("Workspace Matches:", workspace_matches)
print("Dataflow Matches:", dataflow_matches)
# Combine and sort all matches by position
all_matches = sorted(workspace_matches + dataflow_matches, key=lambda x: x[0])
print("All Matches:", all_matches)
# Traverse and build mapping
workspace_to_dataflows = defaultdict(list)
current_workspace = None

for _, kind, guid in all_matches:
    print(current_workspace)

    if kind == "workspaceId":
        current_workspace = guid
    elif kind == "dataflowId" and current_workspace:
        workspace_to_dataflows[current_workspace].append(guid)

# Convert defaultdict to regular dict for output
workspace_to_dataflows = dict(workspace_to_dataflows)
print(workspace_to_dataflows)
