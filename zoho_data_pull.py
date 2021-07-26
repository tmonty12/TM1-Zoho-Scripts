"""
Usage:
    zoho_data_pull <pipeline_file_path> <column_keys> <err_file_path> [<zoho_client_id>] [<zoho_client_secret>] [<zoho_refresh_token>]
    zoho_data_pull -h
    zoho_data_pull --version

Options:
    -h          Show this screen
    --version   Show version
"""

import os
from docopt import docopt
from Zoho import Zoho
from settings import CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN, write_err_msg

def main(args: dict):
    """ This program loads the potentials data from Zoho and writes
        out the data for the given columns to the given path in a 
        csv format

        Parameters
        ----------
        pipeline_file_path: the path to where the pipeline csv file will be written
        column_keys: the keys for the columns that are to be written to the file
    """
    client_id = args['<zoho_client_id>'] if args['<zoho_client_id>'] else CLIENT_ID
    client_secret = args['<zoho_client_secret>'] if args['<zoho_client_secret>'] else CLIENT_SECRET
    refresh_token = args['<zoho_refresh_token>'] if args['<zoho_refresh_token>'] else REFRESH_TOKEN

    if not client_id or not client_secret or not refresh_token:
        raise ValueError('Zoho credentials were not provided.')

    if not args['<column_keys>']:
        raise ValueError('No columns keys were selected')

    keys = args['<column_keys>'].split(',')
    zoho = Zoho(client_id, client_secret, refresh_token)

    path = os.path.abspath(args['<pipeline_file_path>'])
    if os.path.isfile(path):
        os.remove(path)

    with open(path, 'w') as file:
        for i, key in enumerate(keys):
            file.write(key)
            if i < len(keys) - 1:
                file.write(';')

        file.write('\n')

        zoho.write_potentials(file, keys)


if __name__ == "__main__":
    args = docopt(__doc__, version='1.0.0')
    try:
        main(args)
    except KeyError as e:
        err_msg = 'KeyError: The column key {} you provided does not exist in Pipeline file'.format(e)
        write_err_msg(args, err_msg)
    except Exception as e:
        err_msg = '{}: {}'.format(type(e).__name__, e)
        write_err_msg(args, err_msg)