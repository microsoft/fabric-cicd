import re
from pathlib import Path

file_name = "C:\\Data\\Source\\Repos\\ba_almtest\\fabricitems\\f947ed01-6b71-b86c-4856-5aaadb82d2e4.SemanticModel\\definition\\expressions.tmdl"
file_path = Path(file_name)
raw_file = file_path.read_text()
print(raw_file)

key = "DataSourceServer"
parameter_dict = "Nieuwedatabase.database.windows.net"

pattern_str = "expression\\s*" + key + '\\s*\=\\s*"[^"()]*"'
replace_str = "expression " + key + ' = "' + parameter_dict + '"'

pattern_str = "expression\\s*" + key + '\\s*\\=\\s*"[^"()]*"\\s*meta\\s*\[IsParameterQuery\\s=\\strue'
replace_str = "expression " + key + ' = "' + parameter_dict + '" meta \[IsParameterQuery=true'


print(pattern_str)
print(replace_str)
print()
raw_file = re.sub(pattern_str, replace_str, raw_file)
print(raw_file)


key = "DataSourceDatabase"
parameter_dict = "Nieuwedatabase"

pattern_str = "expression\\s*" + key + '\\s*\=\\s*"[^"()]*"'
replace_str = "expression " + key + ' = "' + parameter_dict + '"'

print(pattern_str)
print(replace_str)
print()
raw_file = re.sub(pattern_str, replace_str, raw_file)
print(raw_file)
