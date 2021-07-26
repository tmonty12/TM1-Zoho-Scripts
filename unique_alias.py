"""
Usage:
    unique_alias <file_path> <id_column_name> <filter_columns> <sort_by_column> <sep> <err_file_path>
    unique_alias -h
    unique_alias --version

Options:
    -h          Show this screen
    --version   Show version
"""
import pandas as pd
from os import path
from docopt import docopt
from TM1py import TM1Service
from settings import write_err_msg


def main(args: dict):
    """ This program intakes the Pipeline.csv file and will create a unique id column
        that increments rows with the same data in given columns.

        Parameters
        ----------
        file_path : the path to the Pipeline.csv
        id_column_name : the name of the unique alias id column
        filter_columns : the name of the columns that are used to create the alias
        sort_by_column : an additional column used to sort duplicates (usually timestamp)
        sep : the separator used for the csv file
    """

    file_path = path.abspath(args['<file_path>'])
    df = pd.read_csv(file_path, sep=args['<sep>'])
    filter_columns = args['<filter_columns>'].split(',')
    
    # Create the id column, create duplicate df, sort so that duplicate rows are next to eachother
    df[args['<id_column_name>']] = 0
    duplicates = df.loc[df.duplicated(filter_columns, keep=False)]

    duplicates = duplicates.sort_values(by=filter_columns + [args['<sort_by_column>']])[{*filter_columns}]

    # If the cur_row matches the prev_row, we want to increment the unique id column
    prev_index = 0
    prev_row = dict.fromkeys(duplicates.iloc[0].keys())
    for index, row in duplicates.iterrows():
        counter = 0
        for key in row.keys():
            if prev_row[key] == row[key]:
                counter += 1
        if counter == len(row.keys()):
            df.loc[index, args['<id_column_name>']] = df.loc[prev_index, args['<id_column_name>']] + 1
        prev_row = row
        prev_index = index

    # Output df to same file
    df.to_csv(file_path, sep=args['<sep>'], index=False)


if __name__ == "__main__":
    args = docopt(__doc__, version='1.00')
    try:
        main(args)
    except KeyError as e:
        err_msg = 'KeyError: The column key {} you provided does not exist in Pipeline file'.format(e)
        write_err_msg(args, err_msg)
    except Exception as e:
        err_msg = '{}: {}'.format(type(e).__name__, e)
        write_err_msg(args, err_msg)