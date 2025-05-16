[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared #"Dataprod DIM_Calendar" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "c4e5f401-8e98-467b-b2d8-e097f09bb70a"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "f2809ec9-fb91-4d4b-a6c5-62d0805d102a"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Dataprod.DIM_Calendar", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
