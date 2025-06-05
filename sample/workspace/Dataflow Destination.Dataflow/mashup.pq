[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Dataflow Source_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"Dataflow Source" = let
  Source = PowerPlatform.Dataflows([]),
  #"Navigation 1" = Source{[Id = "Workspaces"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[workspaceId = "82a46da2-618c-4ca4-8d48-96d18da8d685"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[dataflowId = "5f3aae77-f860-4a23-9b75-5b15f888494b"]}[Data],
  #"Navigation 4" = #"Navigation 3"{[entity = "Mapping_ProductMaster", version = ""]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 4", each [CustomProduct] <> null and [CustomProduct] <> ""),
  #"Inserted conditional column" = Table.AddColumn(#"Filtered rows", "Is PBI", each if [CustomProduct] = "Power BI" then "Yes" else "No")
in
  #"Inserted conditional column";
shared #"Dataflow Source_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "82a46da2-618c-4ca4-8d48-96d18da8d685"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "31dcc456-a6b6-46e2-824b-24f1e44b7297"]}[Data],
  TableNavigation = Navigation_2{[Id = "Destination", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
