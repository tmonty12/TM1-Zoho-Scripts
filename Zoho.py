from io import TextIOWrapper
from typing import Tuple
import requests
import time
import json

class Zoho:
    access_token_url = 'https://accounts.zoho.com/oauth/v2/token'
    potentials_url = 'https://www.zohoapis.com/crm/v2/Potentials'
    per_page = 200


    def __init__(self, client_id: str, client_secret: str, refresh_token: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.token_expiration = 0

        self.refresh_access_token()


    def refresh_access_token(self):
        if time.time() > self.token_expiration:
            params = {
                'refresh_token': self.refresh_token,
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'grant_type': 'refresh_token'
            }
            response = requests.post(self.access_token_url, params=params)
            response.raise_for_status()
            json_data = json.loads(response.text)
            
            if 'error' in json_data:
                raise requests.RequestException('Invalid Zoho credentials provided')

            self.access_token = json_data['access_token']
            self.headers = { 'Authorization': 'Zoho-oauthtoken ' + self.access_token }
            self.token_expiration = time.time() + json_data['expires_in_sec']


    def write_potentials(self, file: TextIOWrapper, keys: list[str]):
        self.refresh_access_token()

        page = 1
        more_records = True
        
        # Use a request session for multiple requests
        with requests.Session() as session:
            session.headers.update(self.headers)
            # Continue to query api while there is more potential records
            while more_records:
                data, more_records = self.get_potentials(session, page)
                for data_item in data:
                    for i, key in enumerate(keys):
                        # Account Name and Contact Name have a special JSON format
                        if key == 'Account_Name' or key == 'Contact_Name':
                            if data_item[key] and data_item[key]['name']:
                                file.write(str(data_item[key]['name']))
                        else:
                            if data_item[key]:
                                file.write(str(data_item[key]))
                        
                        if i < len(keys) - 1:
                            file.write(';')
                    file.write('\n')
                page += 1


    def get_potentials(self, session: requests.Session, page: int) ->  Tuple[list[dict], bool]:
        params = {
            'page': page,
            'per_page': self.per_page
        }
        response = session.get(self.potentials_url, params=params)
        response.raise_for_status()
        data = json.loads(response.text)
        
        return (data['data'], data['info']['more_records'])
    

    def update_potentials(self, data):
        self.refresh_access_token()

        response = requests.put(self.potentials_url, data=data, headers=self.headers)
        response.raise_for_status()
        
