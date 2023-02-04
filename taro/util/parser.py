import re
from typing import Dict


class KVParser:
    def __init__(self,
                 prefix: str = "",
                 field_split: str = " ",
                 value_split: str = "=",
                 trim_key: str = None,
                 trim_value: str = None,
                 include_brackets: bool = True):
        """
        :param prefix:
            A string to prepend to all the extracted keys. Default is "".
        :param field_split:
            A string of characters to use as single-character field delimiters for parsing out key-value pairs.
        :param value_split:
            A non-empty string of characters to use as single-character value delimiters
            for parsing out key-value pairs..
        :param trim_key:
            A string of characters to trim from the key. Only leading and trailing characters are trimmed from the key.
        :param trim_value:
            A string of characters to trim from the value. Only leading and trailing characters
            are trimmed from the value.
        ":param include_brackets:
            A boolean specifying whether to treat square brackets, angle brackets, and parentheses as value "wrappers"
            that should be removed from the value.
        """
        self.prefix = prefix
        self.field_split = field_split
        self.value_split = value_split
        self.trim_key = trim_key
        self.trim_value = trim_value
        self.include_brackets = include_brackets

    def _extract_and_remove_bracket_kv(self, text):
        pattern = re.compile(fr'([^{self.field_split}]+)({self.value_split})(\(([^()]+)\)|\[([^\[\]]+)]|<([^<>]+)>)')
        fields = []
        while True:
            match = re.search(pattern, text)
            if not match:
                break
            start, end = match.span()
            fields.append(re.sub(r'[()<>\[\]]', '', match.group(0)))
            text = text[:start] + text[end:]
        return fields, text

    def parse(self, text: str) -> Dict[str, str]:
        result = {}
        if self.include_brackets:
            fields, text = self._extract_and_remove_bracket_kv(text)
        else:
            fields = []

        fields += re.split(self.field_split, text)
        for field in fields:
            key_value = re.split(self.value_split, field, maxsplit=1)
            if len(key_value) == 2:
                key, value = key_value
                if self.trim_key:
                    key = re.sub("^[{}]+|[{}]+$".format(self.trim_key, self.trim_key), "", key)
                if self.trim_value:
                    value = re.sub("^[{}]+|[{}]+$".format(self.trim_value, self.trim_value), "", value)
                result[self.prefix + key] = value
        return result
