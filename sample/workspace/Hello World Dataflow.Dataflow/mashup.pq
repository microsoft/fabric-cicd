[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared Table = let
  Source = Table.FromRows(Json.Document(Binary.Decompress(Binary.FromText("i45W8s9LVYrViVYKKc9Xio0FAA==", BinaryEncoding.Base64), Compression.Deflate)), let _t = ((type nullable text) meta [Serialized.Text = true]) in type table [Example = _t])
in
  Source;
