[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared Table = let
  Source = Table.FromRows(Json.Document(Binary.Decompress(Binary.FromText("i45W8ssvSU3Kz89W0lEyBGKP1JycfIXw/KKcFKVYnWgll8SSxLSc/HKglBFMGi6hEJBZkJqTmZcKlDEGYoS24NTcxLySzGQF3/yU1ByglAkQg9kKqEZAzTYFYhc3mL2xAA==", BinaryEncoding.Base64), Compression.Deflate)), let _t = ((type nullable text) meta [Serialized.Text = true]) in type table [Item = _t, Id = _t, Name = _t]),
  #"Changed column type" = Table.TransformColumnTypes(Source, {{"Item", type text}, {"Id", Int64.Type}, {"Name", type text}}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Changed column type", "IsDataflow", each if [Item] = "Dataflow" then true else false), {{"IsDataflow", type logical}}),
  #"Filtered rows" = Table.SelectRows(#"Added custom", each ([IsDataflow] = true))
in
  #"Filtered rows";
