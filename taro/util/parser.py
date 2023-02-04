import re
from typing import Dict, Set


class KVParser:

    def __init__(self,
                 prefix: str = "",
                 field_split: str = " ",
                 value_split: str = "=",
                 trim_key: str = '',
                 trim_value: str = '',
                 include_brackets: bool = True,
                 exclude_keys: Set[str] = ()):
        """
        :param prefix:
            A string to prepend to all the extracted keys. Default is "".
        :param field_split:
            A string of characters to use as single-character field delimiters for parsing out key-value pairs.
        :param value_split:
            A non-empty string of characters to use as single-character value delimiters
            for parsing out key-value pairs.
        :param trim_key:
            A string of characters to trim from the key. Only leading and trailing characters are trimmed from the key.
        :param trim_value:
            A string of characters to trim from the value. Only leading and trailing characters
            are trimmed from the value.
        :param include_brackets:
            A boolean specifying whether to treat square brackets, angle brackets, and parentheses as value "wrappers"
            that should be removed from the value.
        :param exclude_keys:
            An array specifying the parsed keys which should not be added to the result.
        """
        self.prefix = prefix
        self._field_split = field_split
        self._value_split = value_split
        self.trim_key = trim_key
        self.trim_value = trim_value
        self.include_brackets = include_brackets
        self._bracket_kv_pattern = None
        self._compile_bracket_kv_pattern()
        self._brackets_pattern = re.compile(r'[()<>\[\]]')
        self.exclude_keys = exclude_keys

    def _compile_bracket_kv_pattern(self):
        self._bracket_kv_pattern = re.compile(
            fr'([^{self._field_split}]+)({self._value_split})(\(([^()]+)\)|\[([^\[\]]+)]|<([^<>]+)>)')

    @property
    def field_split(self):
        return self._field_split

    @field_split.setter
    def field_split(self, value):
        self._field_split = value
        self._compile_bracket_kv_pattern()

    @property
    def value_split(self):
        return self._field_split

    @value_split.setter
    def value_split(self, value):
        self._value_split = value
        self._compile_bracket_kv_pattern()

    def _extract_and_remove_bracket_kv(self, text):
        fields = []
        while True:
            match = re.search(self._bracket_kv_pattern, text)
            if not match:
                break
            start, end = match.span()
            fields.append(re.sub(self._brackets_pattern, '', match.group(0)))
            text = text[:start] + text[end:]
        return fields, text

    def parse(self, text: str) -> Dict[str, str]:
        result = {}
        if self.include_brackets:
            fields, text = self._extract_and_remove_bracket_kv(text)
        else:
            fields = []

        fields += re.split(self._field_split, text)
        for field in fields:
            key_value = re.split(self._value_split, field, maxsplit=1)
            if len(key_value) == 2:
                key, value = key_value
                if key in self.exclude_keys:
                    continue
                if self.trim_key:
                    key = key.strip(self.trim_key)
                if self.trim_value:
                    value = value.strip(self.trim_value)
                result[self.prefix + key] = value
        return result
