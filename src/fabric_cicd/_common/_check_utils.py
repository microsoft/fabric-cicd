from importlib.metadata import version as lib_version

import filetype
import requests
from colorama import Fore, Style
from packaging import version


def check_version():
    try:
        current_version = lib_version("fabric-cicd")
        response = requests.get("https://pypi.org/pypi/fabric-cicd/json")
        latest_version = response.json()["info"]["version"]
        if version.parse(current_version) < version.parse(latest_version):
            msg = (
                f"{Fore.BLUE}[notice]{Style.RESET_ALL} A new release of fabric-cicd is available: "
                f"{Fore.RED}{current_version}{Style.RESET_ALL} -> {Fore.GREEN}{latest_version}{Style.RESET_ALL}"
            )
            print(msg)
    except:
        pass


def is_image_file(file_path):
    kind = filetype.guess(file_path)
    if kind is None:
        return False
    return kind.mime.startswith("image/")
