VALID_PARAMETER_FILE_STRING = """
find_replace:
    # Required Fields
    - find_value: "db52be81-c2b2-4261-84fa-840c67f4bbd0" 
      replace_value:
          PPE: "81bbb339-8d0b-46e8-bfa6-289a159c0733"
          PROD: "5d6a1b16-447f-464a-b959-45d0fed35ca0"
      # Optional Fields
      item_type: "Notebook"
      item_name: "Hello World"
      file_path: "/Hello World.Notebook/notebook-content.py"
spark_pool:
    # Required Fields
    - instance_pool_id: "72c68dbc-0775-4d59-909d-a47896f4573b"
      replace_value:
          PPE:
              type: "Capacity"
              name: "CapacityPool_Large_PPE"
          PROD:
              type: "Capacity"
              name: "CapacityPool_Large_PROD"
      # Optional Fields
      item_name: 
"""


VALID_PARAMETER_FILE_ARRAY = """
find_replace:
    # Required Fields
    - find_value: "db52be81-c2b2-4261-84fa-840c67f4bbd0" 
      replace_value:
          PPE: "81bbb339-8d0b-46e8-bfa6-289a159c0733"
          PROD: "5d6a1b16-447f-464a-b959-45d0fed35ca0"
      # Optional Fields
      item_type: ["Environment", "Notebook"]
      item_name:
       - "Hello World"
       - "World"
      file_path: 
       - "/Hello World.Notebook/notebook-content.py"
       - "\\World.Environment\\Setting\\Sparkcompute.yml"
spark_pool:
    # Required Fields
    - instance_pool_id: "72c68dbc-0775-4d59-909d-a47896f4573b"
      replace_value:
          PPE:
              type: "Capacity"
              name: "CapacityPool_Large_PPE"
          PROD:
              type: "Capacity"
              name: "CapacityPool_Large_PROD"
      # Optional Fields
      item_name: ["World"]
"""
logging_messages = [
    "Validating the parameters",
    "Validating find_replace parameter",
    "find_replace parameter validation passed",
    "Validating spark_pool parameter",
    "spark_pool parameter validation passed",
    "Parameter file validation passed",
]


INVALID_PARAMETER_FILE_KEYS = """
find_replace_new:
    # Required Fields
    - find_value: "db52be81-c2b2-4261-84fa-840c67f4bbd0" 
      replace_value:
          PPE: "81bbb339-8d0b-46e8-bfa6-289a159c0733"
          PROD: "5d6a1b16-447f-464a-b959-45d0fed35ca0"
      # Optional Fields
      item_type: "Notebook"
      item_name: "Hello World"
      file_path: "/Hello World.Notebook/notebook-content.py"
spark_pool:
    # Required Fields
    - instance_pool_id: "72c68dbc-0775-4d59-909d-a47896f4573b"
      replace_value:
          PPE:
              type: "Capacity"
              name: "CapacityPool_Large_PPE"
          PROD:
              type: "Capacity"
              name: "CapacityPool_Large_PROD"
      # Optional Fields
      item_name: 
"""
logging_messages = [
    "Validating the parameters",
    "Invalid parameter 'find_replace_new' in the parameter file",
    "Parameter file validation failed",
]

INVALID_PARAMETER_FILE_MISSING_KEYS = """
find_replace:
    # Required Fields 
      replace_value:
          PPE: "81bbb339-8d0b-46e8-bfa6-289a159c0733"
          PROD: "5d6a1b16-447f-464a-b959-45d0fed35ca0"
      # Optional Fields
      item_type: "Notebook"
      item_name: "Hello World"
      file_path: "/Hello World.Notebook/notebook-content.py"
spark_pool:
    # Required Fields
    - instance_pool_id: "72c68dbc-0775-4d59-909d-a47896f4573b"
      # Optional Fields
      item_name: 
"""
logging_messages = ["Validation skipped for old parameter structure"]

INVALID_PARAMETER_FILE_INVALID_KEYS = """
find_replace:
    # Required Fields
    - find_value_1: "db52be81-c2b2-4261-84fa-840c67f4bbd0" 
      replace_value:
          PPE: "81bbb339-8d0b-46e8-bfa6-289a159c0733"
          PROD: "5d6a1b16-447f-464a-b959-45d0fed35ca0"
      # Optional Fields
      item_type: "Notebook"
      item_name: "Hello World"
      file_path: "/Hello World.Notebook/notebook-content.py"
spark_pool:
    # Required Fields
    - instance_pool_id: "72c68dbc-0775-4d59-909d-a47896f4573b"
      replace_value:
          PPE:
              type: "Capacity"
              name: "CapacityPool_Large_PPE"
          PROD:
              type: "Capacity"
              name: "CapacityPool_Large_PROD"
      # Optional Fields
      item_name: 
      file_path:
"""
logging_messages = [
    "Validating the parameters",
    "Validating find_replace parameter",
    "Error in find_replace: Missing required keys",
    "Validating spark_pool parameter",
    "Error in spark_pool: Invalid keys found",
    "Parameter file validation failed",
]
INVALID_PARAMETER_FILE_MISSING_FIND_VALUE = """
find_replace:
    # Required Fields
    - find_value:  
      replace_value:
          PPE: "81bbb339-8d0b-46e8-bfa6-289a159c0733"
          PROD: "5d6a1b16-447f-464a-b959-45d0fed35ca0"
      # Optional Fields
      item_type: "Notebook"
      item_name: "Hello World"
      file_path: "/Hello World.Notebook/notebook-content.py"
spark_pool:
    # Required Fields
    - instance_pool_id:
      replace_value:
          PPE:
              type: "Capacity"
              name: "CapacityPool_Large_PPE"
          PROD:
              type: "Capacity"
              name: "CapacityPool_Large_PROD"
      # Optional Fields
      item_name: 
"""

logging_messages = [
    "Validating the parameters",
    "Validating find_replace parameter"
    "Error in find_replace: Missing value for find_value key"
    "Validating spark_pool parameter"
    "Error in spark_pool: Missing value for instance_pool_id key"
    "Parameter file validation failed",
]
INVALID_PARAMETER_FILE_MISSING_REPLACE_VALUE = """
find_replace:
    # Required Fields
    - find_value: "db52be81-c2b2-4261-84fa-840c67f4bbd0" 
      replace_value:
      # Optional Fields
      item_type: "Notebook"
      item_name: "Hello World"
      file_path: "/Hello World.Notebook/notebook-content.py"
spark_pool:
    # Required Fields
    - instance_pool_id: "72c68dbc-0775-4d59-909d-a47896f4573b"
      replace_value:
      # Optional Fields
      item_name: 
"""

INVALID_PARAMETER_FILE_MISSING_REPLACE_VALUE_ENVIRONMENT = """
find_replace:
    # Required Fields
    - find_value: "db52be81-c2b2-4261-84fa-840c67f4bbd0" 
      replace_value:
          PPE: 
          PROD: "5d6a1b16-447f-464a-b959-45d0fed35ca0"
      # Optional Fields
      item_type: "Notebook"
      item_name: "Hello World"
      file_path: "/Hello World.Notebook/notebook-content.py"
spark_pool:
    # Required Fields
    - instance_pool_id: "72c68dbc-0775-4d59-909d-a47896f4573b"
      replace_value:
          PPE:
              type: "Capacity"
              name: "CapacityPool_Large_PPE"
          PROD:
      # Optional Fields
      item_name: 
"""

INVALID_PARAMETER_FILE_MISSING_REPLACE_VALUE_INVALID_KEY = """
find_replace:
    # Required Fields
    - find_value: "db52be81-c2b2-4261-84fa-840c67f4bbd0" 
      replace_value:
          PPE: "81bbb339-8d0b-46e8-bfa6-289a159c0733"
          PROD: "5d6a1b16-447f-464a-b959-45d0fed35ca0"
      # Optional Fields
      item_type: "Notebook"
      item_name: "Hello World"
      file_path: "/Hello World.Notebook/notebook-content.py"
spark_pool:
    # Required Fields
    - instance_pool_id: "72c68dbc-0775-4d59-909d-a47896f4573b"
      replace_value:
          PPE:
              type_1: "Capacity"
              name: "CapacityPool_Large_PPE"
          PROD:
              type: "Capacity"
              type_2: "Workspace"
              name: "CapacityPool_Large_PROD"
      # Optional Fields
      item_name: 
"""

INVALID_PARAMETER_FILE_INVALID_TYPE = """
find_replace:
    # Required Fields
    - find_value: "db52be81-c2b2-4261-84fa-840c67f4bbd0" 
      replace_value:
          PPE: "81bbb339-8d0b-46e8-bfa6-289a159c0733"
          PROD: "5d6a1b16-447f-464a-b959-45d0fed35ca0"
      # Optional Fields
      item_type: "Notebook"
      item_name: "Hello World"
      file_path: "/Hello World.Notebook/notebook-content.py"
spark_pool:
    # Required Fields
    - instance_pool_id: "72c68dbc-0775-4d59-909d-a47896f4573b"
      replace_value:
          PPE:
              type: "NO_CAPACITY"
              name: "CapacityPool_Large_PPE"
          PROD:
              type: 
              name: "CapacityPool_Large_PROD"
      # Optional Fields
      item_name: 
"""

INVALID_PARAMETER_FILE_MISSING_NAME = """
find_replace:
    # Required Fields
    - find_value: "db52be81-c2b2-4261-84fa-840c67f4bbd0" 
      replace_value:
          PPE: "81bbb339-8d0b-46e8-bfa6-289a159c0733"
          PROD: "5d6a1b16-447f-464a-b959-45d0fed35ca0"
      # Optional Fields
      item_type: "Notebook"
      item_name: "Hello World"
      file_path: "/Hello World.Notebook/notebook-content.py"
spark_pool:
    # Required Fields
    - instance_pool_id: "72c68dbc-0775-4d59-909d-a47896f4573b"
      replace_value:
          PPE:
              type: "Capacity"
              name: "CapacityPool_Large_PPE"
          PROD:
              type: "Capacity"
              name: 
      # Optional Fields
      item_name: 
"""

INVALID_PARAMETER_FILE_INVALID_DATA_TYPE_REQUIRED = """
find_replace:
    # Required Fields
    - find_value: {"db52be81-c2b2-4261-84fa-840c67f4bbd0"} 
      replace_value:
          PPE: "81bbb339-8d0b-46e8-bfa6-289a159c0733"
          PROD: "5d6a1b16-447f-464a-b959-45d0fed35ca0"
      # Optional Fields
      item_type: "Notebook"
      item_name: "Hello World"
      file_path: "/Hello World.Notebook/notebook-content.py"
spark_pool:
    # Required Fields
    - instance_pool_id: "72c68dbc-0775-4d59-909d-a47896f4573b"
      replace_value:
          PPE:
              type: "Capacity"
              name: ["CapacityPool_Large_PPE"]
          PROD:
              type: "Capacity"
              name: "CapacityPool_Large_PROD"
      # Optional Fields
      item_name: 
"""

INVALID_PARAMETER_FILE_INVALID_DATA_TYPE_OPTIONAL = """
find_replace:
    # Required Fields
    - find_value: "db52be81-c2b2-4261-84fa-840c67f4bbd0" 
      replace_value:
          PPE: "81bbb339-8d0b-46e8-bfa6-289a159c0733"
          PROD: "5d6a1b16-447f-464a-b959-45d0fed35ca0"
      # Optional Fields
      item_type: "Notebook"
      item_name: ["Hello World", "World"]
      file_path: {"/Hello World.Notebook/notebook-content.py"}
spark_pool:
    # Required Fields
    - instance_pool_id: "72c68dbc-0775-4d59-909d-a47896f4573b"
      replace_value:
          PPE:
              type: "Capacity"
              name: "CapacityPool_Large_PPE"
          PROD:
              type: "Capacity"
              name: "CapacityPool_Large_PROD"
      # Optional Fields
      item_name: ("World", "Hello")
"""

INVALID_PARAMETER_FILE_INVALID_OPTIONAL_VALUES = """
find_replace:
    # Required Fields
    - find_value: "db52be81-c2b2-4261-84fa-840c67f4bbd0" 
      replace_value:
          PPE: "81bbb339-8d0b-46e8-bfa6-289a159c0733"
          PROD: "5d6a1b16-447f-464a-b959-45d0fed35ca0"
      # Optional Fields
      item_type: "SparkNotebook"
      item_name: "Hello World"
      file_path: 
       - "/Hello World.Notebook/notebook-content/.py"
       - "\\Hello World 2.Notebook\\notebook-content.py"
spark_pool:
    # Required Fields
    - instance_pool_id: "72c68dbc-0775-4d59-909d-a47896f4573b"
      replace_value:
          PPE:
              type: "Capacity"
              name: "CapacityPool_Large_PPE"
          PROD:
              type: "Capacity"
              name: "CapacityPool_Large_PROD"
      # Optional Fields
      item_name: "WORLD"
"""
