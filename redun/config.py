import os
from configparser import ConfigParser, ExtendedInterpolation, SectionProxy
from typing import Any, Dict, Iterable, Optional, Union

from redun.file import File


class RedunExtendedInterpolation(ExtendedInterpolation):
    """
    When performing variable interpolation fallback to environment variables.

    For example, if the environment variable ROLE is defined, we can reference
    it in the `redun.ini` file as follows:

    .. code-block:: ini
        [executors.batch]
        role = ${ROLE}
    """

    def before_get(self, parser, section, option, value, defaults):
        # Fallback to environment variables when interpolating variables.
        defaults = {
            **defaults,
            **os.environ,
        }
        return super().before_get(parser, section, option, value, defaults)


class RedunConfigParser(ConfigParser):
    def optionxform(self, optionstr):
        # Treat option names as case sensitive.
        return optionstr


class Config:
    """
    Extends ConfigParser to support nested sections.
    """

    def __init__(self, config_dict: Optional[dict] = None):
        self.parser = RedunConfigParser(interpolation=RedunExtendedInterpolation())
        self._sections: "Section" = {}
        if config_dict:
            self.read_dict(config_dict)

    def read_string(self, string: str) -> None:
        self.parser.read_string(string)
        self._sections = self._parse_sections(self.parser)

    def read_path(self, filename: str) -> None:
        with File(filename).open() as infile:
            self.parser.read_file(infile)
            self._sections = self._parse_sections(self.parser)

    def read_dict(self, config_dict: dict) -> None:
        self.parser.read_dict(config_dict)
        self._sections = self._parse_sections(self.parser)

    def _parse_sections(self, parser) -> Union[dict, "Section"]:
        """
        Parse a dot notation section into nested dicts.
        """
        full_sections = parser.sections()
        nested_sections: dict = {}
        for full_section in full_sections:
            parts = full_section.split(".")
            ptr = nested_sections
            for part in parts[:-1]:
                if part not in ptr:
                    ptr[part] = {}
                ptr = ptr[part]

            ptr[parts[-1]] = parser[full_section]
        return nested_sections

    def get(self, key: str, default: Any = None) -> Any:
        return self._sections.get(key, default)

    def __getitem__(self, section_name: str) -> Any:
        return self._sections[section_name]

    def __setitem__(self, section_name: str, section: "Section") -> "Section":
        # TODO: See how to properly get type checking for this assignment.
        self._sections[section_name] = section  # type: ignore
        return section

    def keys(self) -> Iterable[str]:
        return self._sections.keys()

    def items(self):
        return self._sections.items()

    def get_config_dict(self, replace_config_dir=None) -> Dict[str, Dict]:
        """Return a python dict that can be used to reconstruct the Config object

        config2 = Config(config_dict=config1.get_config_dict()) should result in identical
        Configs config1 and config2.

        Note: The structure of the returned Dict is essentially a two-level dict corresponding to
        INI file structure. Top-level key is a dot-separated section name.  Top-level value is a
        single-level dict containing key/values for a single section.

        Parameters
        ----------
        replace_config_dir
            if not None, replaces any variables containing the machine-local config_dir (as
            obtained by `redun.cli.get_config_dir()` with `replace_config_dir`.
            Typical values that are replaced are `backend::config_dir`, `db_uri`,
            `repos.default::config_dir`.

        Returns
        -------
        A copy of this object's config in two-level format, optionally with values containing
        the local config directory replaced.
        """
        from redun.cli import get_config_dir

        result = {}
        local_config_dir = get_config_dir()

        def substitute_config_dir(s):
            # Replace string values containing local config_dir with the provided one
            if replace_config_dir is not None and isinstance(s, str):
                return s.replace(local_config_dir, replace_config_dir)
            return s

        def convert_to_dict(path, obj):
            if isinstance(obj, SectionProxy):
                result[path] = {k: substitute_config_dir(v) for k, v in obj.items()}
                return
            for key in obj.keys():
                convert_to_dict(f"{path}.{key}" if path else key, obj[key])

        convert_to_dict("", self)
        return result


Section = Union[dict, SectionProxy, Config]


def create_config_section(config_dict: Optional[dict] = None) -> SectionProxy:
    """
    Create a default section.
    """
    return Config({"section": config_dict or {}})["section"]
