[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared Mapping_ProductMaster = let
  Source = Excel.Workbook(Web.Contents("https://microsoft-my.sharepoint.com/personal/shirasassoon_microsoft_com/Documents/Apps/Microsoft Power Query/Uploaded Files/Mapping_ProductMaster 1.xlsx"), null, true),
  #"Navigation 1" = Source{[Item = "Mapping_ProductMaster", Kind = "Sheet"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(#"Navigation 1", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Type", type text}, {"Organization", type text}, {"Pillar", type text}, {"ProductGroup", type text}, {"ProductMidGroup", type text}, {"Product", type text}, {"CustomProduct", type text}, {"DIM_ProductMasterId", Int64.Type}, {"BAGInternalProductId", Int64.Type}})
in
  #"Changed column type";
