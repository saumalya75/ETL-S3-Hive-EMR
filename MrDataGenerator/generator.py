import os
import sys
import math
import json
import typing
import logging
import pathlib
import tempfile
from columns import Column


logging.basicConfig(level="INFO")


def __generate_data(data_schema: typing.List, number_of_rows: int = 5):
    data_gens = []
    for col in data_schema:
        data_gens.append(col.generate(number_of_rows))
    return data_gens


def __parse_columns(columns: typing.List[typing.Dict]) -> typing.List:
    data_schema = [Column(column) for column in columns]
    return data_schema


def __parse_metadata_file(metadata_file: str):
    metadata_filepath = pathlib.Path(metadata_file)
    if not metadata_filepath.exists():
        error_msg = f"metadata file - {metadata_file} is not available."
        logging.error(error_msg)
        raise Exception(error_msg)

    if not metadata_filepath.suffix.upper() != "JSON":
        error_msg = f"metadata file - {metadata_file} is not having proper extension, valid extension is JSON/json."
        logging.error(error_msg)
        raise Exception(error_msg)

    try:
        with open(metadata_file, "r") as sf:
            metadata = json.load(sf)
    except Exception as e:
        logging.error(str(e))
        raise e

    for col in metadata.get('columns', []):
        if col['type'] == "lookup":
            if 'lookupFile' not in col.keys() or "lookupCol" not in col.keys():
                error_msg = "It is mandatory to provide 'lookupFile' and 'lookupCol' for 'lookup' type column."
                logging.error(error_msg)
                raise Exception(error_msg)
            lookup_filepath = pathlib.Path(col['lookupFile'])
            if not lookup_filepath.exists():
                error_msg = "Lookup file - {} is not available.".format(col['lookupFile'])
                logging.error(error_msg)
                raise Exception(error_msg)

    try:
        for_each_cols = []
        id_cols = []
        rest_cols = []
        for col in metadata.get('columns', []):
            if col.get('idColumn', 'N') == 'Y':
                id_cols.append(col['name'])
            elif col.get('forEach', 'N') == 'Y':
                for_each_cols.append(col['name'])
            else:
                rest_cols.append(col['name'])
        if len(for_each_cols):
            header_row = metadata.get('columnDelimiter', ',').join(
                rest_cols + for_each_cols + id_cols
            )
        else:
            header_row = metadata.get('columnDelimiter', ',').join(
                [col['name'] for col in metadata.get('columns', [])]
            )

        with open(metadata['filePathName'], 'w') as tf:
            tf.write(header_row)
            tf.write("\n")
    except Exception as e:
        logging.error(str(e))
        raise e

    return metadata


def __generate(metadata_file: str):
    metadata = __parse_metadata_file(metadata_file)
    data_schema = __parse_columns(metadata['columns'])
    logging.debug(data_schema)
    sample_data_gens = __generate_data(data_schema)
    logging.debug([metadata.get('columnDelimiter', ',').join(row) for row in zip(*sample_data_gens)])
    sample_data_gens = __generate_data(data_schema)
    with tempfile.NamedTemporaryFile() as tsf:
        with open(tsf.name, 'w+') as tsfw:
            tsfw.writelines([metadata.get('columnDelimiter', ',').join(row) for row in zip(*sample_data_gens)])
        sample_row_size = os.stat(tsf.name).st_size / (1024 * 1024 * 5)
        logging.debug(f"Temporary file size is {sample_row_size}")
    logging.debug("{}, {}".format(
        metadata.get('maxRowCount', 100),
        math.ceil(metadata.get('tentativeFileSize', 1)/sample_row_size)
    ))
    required_number_of_rows = min(
        metadata.get('maxRowCount', 100),
        math.ceil(metadata.get('tentativeFileSize', 1)/sample_row_size)
    )
    if any([col.for_each.upper() == 'Y' for col in data_schema]):
        for_each_cols = [col for col in data_schema if col.for_each.upper() == 'Y']
        for_each_lists = [for_each_col.choices for for_each_col in for_each_cols]
        for_each_dataset = []
        for col in for_each_lists:
            if not for_each_dataset:
                for v in col:
                    for_each_dataset.append(str(v))
            else:
                new_for_each_dataset = []
                for elem in for_each_dataset:
                    for new_elem in col:
                        new_for_each_dataset.append(elem + metadata.get('columnDelimiter', ',') + str(new_elem))
                for_each_dataset = new_for_each_dataset
        logging.debug(for_each_dataset)
        actual_required_number_of_rows = required_number_of_rows
        required_number_of_rows = math.ceil(required_number_of_rows / len(for_each_dataset))
        with open(metadata['filePathName'], 'a') as tf:
            id_data_gens = __generate_data(
                [col for col in data_schema if col.id_flag],
                actual_required_number_of_rows
            )
            logging.info("Writing around {} lines in target file - {}.".format(
                actual_required_number_of_rows,
                metadata['filePathName'])
            )
            row_number = 0
            id_data_list = []
            for row in zip(*id_data_gens):
                id_data_list.append(metadata.get('columnDelimiter', ',').join(row))
            for for_each_row in for_each_dataset:
                data_gens = __generate_data(
                    [col for col in data_schema if col.for_each.upper() != 'Y' and (not col.id_flag)],
                    required_number_of_rows
                )
                for row in zip(*data_gens):
                    if row_number >= len(id_data_list):
                        break
                    tf.write(
                        metadata.get('columnDelimiter', ',').join(row)
                        + metadata.get('columnDelimiter', ',')
                        + for_each_row
                        + metadata.get('columnDelimiter', ',')
                        + id_data_list[row_number]
                    )
                    tf.write("\n")
                    row_number += 1
    else:
        logging.info("Writing {} lines in target file - {}.".format(required_number_of_rows, metadata['filePathName']))
        data_gens = __generate_data(data_schema, required_number_of_rows)
        with open(metadata['filePathName'], 'a') as tf:
            for row in zip(*data_gens):
                tf.write(metadata.get('columnDelimiter', ',').join(row))
                tf.write("\n")
    logging.debug("Done!")


if __name__ == "__main__":
    metadata_file = sys.argv[1]
    __generate(metadata_file)
