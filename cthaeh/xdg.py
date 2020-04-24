import os

from pathlib import Path


def get_home() -> Path:
    try:
        return Path(os.environ['HOME'])
    except KeyError:
        raise Exception('$HOME environment variable not set')


def get_xdg_cache_home() -> Path:
    try:
        return Path(os.environ['XDG_CACHE_HOME'])
    except KeyError:
        return get_home() / '.cache'


def get_xdg_config_home() -> Path:
    try:
        return Path(os.environ['XDG_CONFIG_HOME'])
    except KeyError:
        return get_home() / '.config'


def get_xdg_data_home() -> Path:
    try:
        return Path(os.environ['XDG_DATA_HOME'])
    except KeyError:
        return get_home() / '.local' / 'share'


def get_xdg_cthaeh_root() -> Path:
    """
    Returns the base directory under which cthaeh will store all data.
    """
    try:
        return Path(os.environ['XDG_CTHAEH_ROOT'])
    except KeyError:
        return get_xdg_data_home() / 'cthaeh'
