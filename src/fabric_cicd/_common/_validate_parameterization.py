import logging
from pathlib import Path
from typing import Optional, Union

logger = logging.getLogger(__name__)


def check_replacement_condition(
    input_type: Union[str, list, None],
    input_name: Union[str, list, None],
    input_path: Union[str, list, None],
    item_type: str,
    item_name: str,
    file_path: str,
) -> bool:
    """Determines if a replacement condition is met."""
    # Condition 1: Zero optional inputs present
    if not input_type and not input_name and not input_path:
        return True

    # Set conditions for optional parameters based on input value type
    item_type_condition = _set_replacement_conditions(input_type, item_type)
    item_name_condition = _set_replacement_conditions(input_name, item_name)
    file_path_condition = _set_replacement_conditions(input_path, file_path, "path")

    # List of conditions for replacement
    replace_conditions = [
        # Condition 2: Type, Name, and Path inputs are present and match
        (item_type_condition and item_name_condition and file_path_condition),
        # Condition 3: Only Type and Name inputs are present and match
        (item_type_condition and item_name_condition and not input_path),
        # Condition 4: Only Type and Path inputs are present and match
        (item_type_condition and file_path_condition and not input_name),
        # Condition 5: Only Name and Path inputs are present and match
        (item_name_condition and file_path_condition and not input_type),
        # Condition 6: Only Type input is present and matches
        (item_type_condition and not input_name and not input_path),
        # Condition 7: Only Name input is present and matches
        (item_name_condition and not input_type and not input_path),
        # Condition 8: Only Path input is present and matches
        (file_path_condition and not input_type and not input_name),
    ]

    return any(replace_conditions)


def _set_replacement_conditions(
    input_value: Union[str, Path, list[Union[str, Path]], None],
    compare_value: Union[str, Path],
    input_type: Optional[str] = None,
) -> bool:
    """A helper function to determine the proper replacement condition based on input_value type."""
    input_value == _handle_input_path(input_value) if input_type == "path" else input_value

    if isinstance(input_value, list):
        input_value_condition = any(compare_value == item for item in input_value)
    elif isinstance(input_value, (str, Path)):
        input_value_condition = compare_value == input_value
    else:
        input_value_condition = False

    return input_value_condition


def _handle_input_path(input_path: Union[str, list[str], None]) -> Union[Path, list[Path]]:
    """A helper function to handle input path."""
    if isinstance(input_path, list):
        input_path = [Path(path) for path in input_path]
        for path in input_path:
            if not path.is_absolute():
                absolute_input_path = path.resolve()
                logger.info(f"Relative file path '{path}' resolved as '{absolute_input_path}'")
                input_path[input_path.index(path)] = absolute_input_path
    elif isinstance(input_path, str):
        input_path = Path(input_path)
        if not input_path.is_absolute():
            absolute_input_path = input_path.resolve()
            logger.info(f"Relative file path '{input_path}' resolved as '{absolute_input_path}'")
            input_path = absolute_input_path

    return input_path
