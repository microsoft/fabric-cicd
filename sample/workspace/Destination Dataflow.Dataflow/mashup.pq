[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared #"Source Dataflow" = let
  Source = PowerPlatform.Dataflows([]),
  #"Navigation 1" = Source{[Id = "Workspaces"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[workspaceId = "82a46da2-618c-4ca4-8d48-96d18da8d685"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[dataflowId = "0187104d-7a35-4abe-a2ca-a241ec81c8f1"]}[Data],
  #"Navigation 4" = #"Navigation 3"{[entity = "Table", version = ""]}[Data]
in
  #"Navigation 4";
