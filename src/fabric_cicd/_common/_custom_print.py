import datetime


def _get_ansi_color(color_name):
    """
    Returns the ANSI color code for the given color name.

    :param color_name: The name of the color (e.g., 'red', 'green').
    :return: The ANSI color code as a string. Defaults to white if color not found.
    """
    color_dict = {
        "black": "\033[30m",
        "red": "\033[31m",
        "green": "\033[32m",
        "yellow": "\033[33m",
        "blue": "\033[34m",
        "magenta": "\033[35m",
        "cyan": "\033[36m",
        "white": "\033[37m",
        "gray": "\033[90m",
        "light_red": "\033[91m",
        "light_green": "\033[92m",
        "light_yellow": "\033[93m",
        "light_blue": "\033[94m",
        "light_magenta": "\033[95m",
        "light_cyan": "\033[96m",
        "light_white": "\033[97m",
    }
    return color_dict.get(
        color_name.lower(), "\033[37m"
    )  # Default to white if color not found


def print_line(message, color="white"):
    """
    Prints a message with the specified color.

    :param message: The message to print.
    :param color: The color to use for the message (default is 'white').
    """
    color_begin = _get_ansi_color(color)
    color_end = "\033[0m"
    print(f"{color_begin}{message}{color_end}")


def print_sub_line(message):
    """
    Prints a sub-message indented with a tab.

    :param message: The sub-message to print.
    """
    print_line(f"\t{message}")


def print_timestamp(message):
    """
    Prints a timestamped message.

    :param message: The message to print with a timestamp.
    """
    current_time = datetime.datetime.now().strftime("[%H:%M:%S] ")
    print_line(f"{current_time}{message}")


def print_header(message, color="green"):
    """
    Prints a header message with a decorative line above and below it.

    :param message: The header message to print.
    :param color: The color to use for the header and lines (default is 'green').
    """
    line_separator = "#" * 100
    formatted_message = f"########## {message}"
    formatted_message = f"{formatted_message} {line_separator[len(formatted_message):]}"

    print_line("")  # Print a blank line before the header
    print_line(line_separator, color)
    print_line(formatted_message, color)
    print_line(line_separator, color)
    print_line("")  # Print a blank line after the header
