"""
A Config file reader
"""
import os
import configparser as ConfigParser

# Setting base paths
ROOT_PATH = os.getenv('ROOT_PATH', '/')
HOSTNAME = os.getenv('HOSTNAME', 'default-hostname')
LOG_PATH = (
    os.path.join(ROOT_PATH, 'var', 'log', 'jasmin', HOSTNAME)
    if 'KUBERNETES_SERVICE_HOST' in os.environ
    else os.getenv('LOG_PATH', os.path.join(ROOT_PATH, 'var', 'log', 'jasmin'))
)

class ConfigFile:
    """
    Config file reader to expose typed (e.g., _getint()) and untyped (e.g., _get())
    methods with default fallback values.
    """

    def __init__(self, config_file=None):
        """
        Initialize the ConfigFile object.
        :param config_file: Path to the configuration file.
        """
        self.config_file = config_file
        self.config = ConfigParser.RawConfigParser()

        if self.config_file is not None:
            self.config.read(config_file)

    def getConfigFile(self):
        """Return the current configuration file path."""
        return self.config_file

    def _get(self, section, option, default=None):
        """
        Retrieve a configuration value.
        :param section: Configuration section.
        :param option: Option within the section.
        :param default: Default value if option is not found.
        :return: Value from the configuration or environment variable.
        """
        return self._fetch_value(section, option, default, cast=str)

    def _getint(self, section, option, default=None):
        """
        Retrieve an integer configuration value.
        :param section: Configuration section.
        :param option: Option within the section.
        :param default: Default value if option is not found.
        :return: Integer value from the configuration or environment variable.
        """
        return self._fetch_value(section, option, default, cast=int)

    def _getfloat(self, section, option, default=None):
        """
        Retrieve a float configuration value.
        :param section: Configuration section.
        :param option: Option within the section.
        :param default: Default value if option is not found.
        :return: Float value from the configuration or environment variable.
        """
        return self._fetch_value(section, option, default, cast=float)

    def _getbool(self, section, option, default=None):
        """
        Retrieve a boolean configuration value.
        :param section: Configuration section.
        :param option: Option within the section.
        :param default: Default value if option is not found.
        :return: Boolean value from the configuration or environment variable.
        """
        return self._fetch_value(section, option, default, cast=self._convert_to_bool)

    def _fetch_value(self, section, option, default, cast):
        """
        Generalized method to fetch and cast configuration values.
        :param section: Configuration section.
        :param option: Option within the section.
        :param default: Default value if option is not found.
        :param cast: Callable to cast the retrieved value.
        :return: The casted value.
        """
        env_var = self._convert_to_env_var_str(f"{section}_{option}")
        if env_var in os.environ:
            return cast(os.environ[env_var])

        if not self.config.has_section(section):
            return default

        if not self.config.has_option(section, option):
            return default

        value = self.config.get(section, option)
        if value == 'None':
            return default

        return cast(value)

    def _convert_to_bool(self, value):
        """
        Convert a value to boolean.
        :param value: Value to convert.
        :return: Boolean representation of the value.
        """
        if isinstance(value, str):
            return value.lower() in ['t', 'true', 'yes', 'y', '1']
        return bool(value)

    def _convert_to_env_var_str(self, env_str):
        """
        Convert a string to an environment-friendly uppercase format.
        :param env_str: Input string.
        :return: Converted string.
        """
        return env_str.replace('-', '_').upper()
