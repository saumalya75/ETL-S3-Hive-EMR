import typing
import string
import random
import pandas as pd
import logging as col_logs


col_logs.basicConfig(level="INFO")


class Column:
    def __new__(cls, col_meta: typing.Dict):
        col_type = col_meta.get('type', '')
        if col_type.upper() == 'INTEGER':
            return IntegerColumn(col_meta)
        if col_type.upper() == 'DECIMAL':
            return DecimalColumn(col_meta)
        elif col_type.upper() == 'STRING':
            return StringColumn(col_meta)
        elif col_type.upper() == 'CHOICE':
            return ChoiceColumn(col_meta)
        elif col_type.upper() == 'LOOKUP':
            return LookupColumn(col_meta)
        else:
            col_name = col_meta['name']
            error_msg = f"Selected column type({col_type}) for column - {col_name} is not supported in the system yet."
            col_logs.error(error_msg)
            raise Exception(error_msg)


class BaseColumn:
    def __init__(self, col_meta: typing.Dict):
        self.col_meta = col_meta

    @property
    def id_flag(self):
        return self.col_meta.get('idColumn', 'N') == 'Y'

    @property
    def type(self):
        return self.col_meta['type']

    @property
    def for_each(self):
        return self.col_meta.get('forEach', 'N')

    def generate(self, number_of_rows: int):
        return ('' for _ in range(number_of_rows))


class IntegerColumn(BaseColumn):
    def __repr__(self):
        return "Integer column '{}'.".format(self.col_meta['name'])

    def generate(self, number_of_rows: int):
        min_value = int(self.col_meta.get('minValue', 0))
        max_value = int(self.col_meta.get('minValue', min_value+1000))
        if self.col_meta.get('idColumn', 'N').upper() == "Y":
            return (str(min_value + i) for i in range(number_of_rows))
        return (random.randint(min_value, max_value) for _ in range(number_of_rows))


class DecimalColumn(BaseColumn):
    DECIMAL_PRECISION = 2

    def __repr__(self):
        return "Decimal column '{}'.".format(self.col_meta['name'])

    def generate(self, number_of_rows: int):
        min_value = self.col_meta.get('minValue', 0.0)
        max_value = self.col_meta.get('maxValue', min_value+1000)
        if self.col_meta.get('idColumn', 'N').upper() == "Y":
            return (str(min_value + i) for i in range(number_of_rows))
        return (str(round(random.uniform(min_value, max_value), self.DECIMAL_PRECISION)) for _ in range(number_of_rows))


class StringColumn(BaseColumn):
    def __repr__(self):
        return "String column '{}'.".format(self.col_meta['name'])

    def generate(self, number_of_rows: int):
        min_value = int(self.col_meta.get('minValue', 0))
        if self.col_meta.get('idColumn', 'N').upper() == "Y":
            return (str(min_value + i) for i in range(number_of_rows))
        if int(self.col_meta.get('length', 10)):
            letters = string.ascii_uppercase
            for _ in range(number_of_rows):
                first_part_length = random.choice(range(2, 9))
                second_part_length = 10 - first_part_length
                yield ''.join(random.choice(letters) for i in range(first_part_length)) \
                      + ' ' \
                      + ''.join(random.choice(letters) for i in range(second_part_length))


class ChoiceColumn(BaseColumn):
    def __repr__(self):
        return "Choice column '{}'.".format(self.col_meta['name'])

    @property
    def choices(self):
        return self.col_meta.get('choices', [None])

    def generate(self, number_of_rows: int):
        return (str(random.choice(self.col_meta.get('choices', [None]))) for _ in range(number_of_rows))


class LookupColumn(BaseColumn):
    def __repr__(self):
        return "Choice column '{}'.".format(self.col_meta['name'])

    @property
    def choices(self):
        lkp_file = self.col_meta['lookupFile']
        lkp_col = self.col_meta['lookupCol']
        lkp_del = self.col_meta.get('lookupDelimiter', '|')
        lkp_df = pd.read_csv(lkp_file, delimiter=lkp_del)
        return lkp_df[lkp_col].unique()

    def generate(self, number_of_rows: int):
        return (str(random.choice(self.choices)) for _ in range(number_of_rows))

