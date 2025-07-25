[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared Table = let
  Source = Table.FromRows(Json.Document(Binary.Decompress(Binary.FromText("i45W8kjNyclX0lEyVIrViVYKzy/KSQHyjMA8sJwCTMxYKTYWAA==", BinaryEncoding.Base64), Compression.Deflate)), let _t = ((type nullable text) meta [Serialized.Text = true]) in type table [Hello = _t, Id = _t]),
  #"Changed column type" = Table.TransformColumnTypes(Source, {{"Hello", type text}, {"Id", Int64.Type}})
in
  #"Changed column type";
