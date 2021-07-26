import os

CLIENT_ID = os.getenv('Zoho_Client_Id')
CLIENT_SECRET = os.getenv('Zoho_Client_Secret')
REFRESH_TOKEN = os.getenv('Zoho_Refresh_Token')

def get_server_credentials(server_name: str, server_address: str, server_port: str, server_user: str, server_password: str) -> tuple[str]:
    SERVER_ADDRESS = server_address if server_address else os.getenv('{}_Address'.format(server_name))
    SERVER_PORT = server_port if server_port else os.getenv('{}_Port'.format(server_name))
    SERVER_USER = server_user if server_user else os.getenv('{}_User'.format(server_name))
    SERVER_PASSWORD = server_password if server_password else os.getenv('{}_Password'.format(server_name))

    if SERVER_PASSWORD == None: SERVER_PASSWORD = ''

    if not SERVER_ADDRESS or not SERVER_PORT or not SERVER_USER:
        raise ValueError('Incorrect server credentials provided')

    return (SERVER_ADDRESS, SERVER_PORT, SERVER_USER, SERVER_PASSWORD)


def write_err_msg(args: dict, err_msg):
    path = os.path.abspath(args['<err_file_path>'])
    with open(path, 'w') as f:
        f.write(err_msg)