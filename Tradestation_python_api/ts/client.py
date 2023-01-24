import os
import time
import json

import requests
import urllib.parse

from typing import List
from typing import Dict
# from typing import Union
# from typing import Optional

from datetime import date, datetime, timedelta


class TradeStationClient:

    """
        TradeStation API Client Class.

        Implements OAuth 2.0 Authorization Code Grant workflow, handles configuration
        and state management, adds token for authenticated calls, and performs request 
        to the TD Ameritrade API.
    """

    def __init__(self, username: str, client_id: str, client_secret: str, 
                 redirect_uri: str, version: int, paper_trading: bool = True) -> None:
        """initializes the Tradestation Client object.

        Arguments:
        ----
        username (str): The username of the account.

        client_id (str): The Client ID assigned to you during the App registration. This can
            be found at the app registration portal.

        client_secret (str):  The Client Secret assigned to you during the App registration. This can
            be found at the app registration portal.

        redirect_uri (str): This is the redirect URL that you specified when you created your
            Tradestation Application.

        paper_trading (bool, optional): Specifies whether you want to use the simulation account or not. 
            Defaults to True.

        Usage:
        ----
            >>> trade_station_client = TradeStationClient(
                    username=username,
                    client_id=client_id,
                    client_secret=client_secret,
                    redirect_uri=redirect_uri,
                    paper_trading=paper_trading
                )
            >>> trade_station_client
        """

        # define the configuration settings.
        self.config = {
            'client_id': client_id,
            'client_secret': client_secret,
            'username': username,
            'redirect_uri': redirect_uri,
            'auth_version': version,
            'resource': 'https://api.tradestation.com',
            'paper_resource': 'https://sim-api.tradestation.com',
            'api_version': 'v3',
            'paper_api_version': 'v3',
            'auth_endpoint': 'https://api.tradestation.com/v2/Security/Authorize' if version == 2 else
                             'https://signin.tradestation.com/oauth/token',
            'cache_state': True,
            'refresh_enabled': True,
            'paper_trading': paper_trading
        }

        # Initialize the client to either use paper trading account or regular account
        if self.config['paper_trading']:
            self.paper_trading_mode = True
        else:
            self.paper_trading_mode = False

        # call the _state_manager method and update the state to init (initialized)
        self._state_manager('init')

        # Define a new attribute called 'authstate' and initialize it to '' (Blank)
        # This will be used by our login function.
        self.authstate = False

    def __repr__(self) -> str:
        """Defines the string representation of our TD Ameritrade Class instance.

        Returns:
        ----
        (str): A string representation of the client.
        """

        # Define the string representation.
        str_representation = '<TradeStation Client (logged_in={log_in}, authorized={auth_state})>'.format(
            log_in=self.state['logged_in'],
            auth_state=self.authstate
        )

        return str_representation

    def _headers(self) -> Dict:
        """Sets the headers for the request.

        Overview:
        ----
        Returns a dictionary of default HTTP headers for calls to TradeStation API,
        in the headers we defined the Authorization and access token.

        Arguments:
        ----
        mode (str): Defines the content-type for the headers dictionary.

        Returns:
        ----
        (dict): The headers dictionary to be used in the request.
        """

        # Grab the Access Token.
        token = self.state['access_token']

        # Create the headers dictionary
        headers = {
            'Authorization': 'Bearer {access_token}'.format(access_token=token),
            "content-type": "application/json"
        }

        # # Set the Mode.
        # if mode == 'application/json':
        #     headers['Content-type'] = 'application/json'
        # elif mode == 'chunked':
        #     headers['Transfer-Encoding'] = 'Chunked'

        return headers

    def _api_endpoint(self, url: str) -> str:
        """Creates an API URL.

        Overview:
        ----  
        Convert relative endpoint (e.g., 'quotes') to full API endpoint.

        Arguments:
        ----
        url (str): The URL that needs conversion to a full endpoint URL.

        Returns:
        ---
        (str): A full URL.
        """

        # paper trading uses a different base url compared to regular trading.
        if self.paper_trading_mode:
            full_url = '/'.join([self.config['paper_resource'],
                                 self.config['paper_api_version'], url])
        else:
            full_url = '/'.join([self.config['resource'],
                                 self.config['api_version'], url])

        return full_url

    def _state_manager(self, action: str) -> None:
        """Handles the state.

        Overview:
        ----
        Manages the self.state dictionary. initialize State will set
        the properties to their default value. Save will save the 
        current state if 'cache_state' is set to TRUE.

        Arguments:
        ----
        name (str): action argument must of one of the following:
            'init' -- initialize State.
            'save' -- Save the current state.         
        """

        # Define the initialized state, these are the default values.
        initialized_state = {
            'access_token': None,
            'refresh_token': None,
            'access_token_expires_at': 0,
            'access_token_expires_in': 0,
            'logged_in': False
        }

        # Grab the current directory of the client file, that way we can store the JSON file in the same folder.
        dir_path = os.path.dirname(os.path.realpath(__file__))
        filename = f'{self.config["client_id"]}_ts_state.json'
        file_path = os.path.join(dir_path, filename)

        # If the state is initialized.
        if action == 'init':

            # initialize the state.
            self.state = initialized_state

            # If they allowed for caching and the file exist, load the file.
            if self.config['cache_state'] and os.path.isfile(file_path):
                with open(file=file_path, mode='r') as state_file:
                    self.state.update(json.load(fp=state_file))

            # If they didn't allow for caching delete the file
            elif not self.config['cache_state'] and os.path.isfile(file_path):
                os.remove(file_path)

        # if they want to save it and have allowed for caching then load the file.
        elif action == 'save' and self.config['cache_state']:
            with open(file=file_path, mode='w+') as state_file:
                json.dump(obj=self.state, fp=state_file, indent=4)

    def login(self) -> bool:
        """Logs the user into a new session.

        Overview:
        ---
        Ask the user to authenticate  themselves via the TD Ameritrade Authentication Portal. This will
        create a URL, display it for the User to go to and request that they paste the final URL into
        command window.

        Once the user is authenticated the API key is valid for 90 days, so refresh tokens may be used
        from this point, up to the 90 days.

        Returns:
        ----
        (bool): `True` if the session was logged in, `False` otherwise.
        """

        # if caching is enabled then attempt silent authentication.
        if self.config['cache_state']:

            # if it was successful, the user is authenticated.
            if self._silent_sso():

                # update the authentication state
                self.authstate = True
                return True

        # Go through the authorization process.
        self._authorize()

        # Grab the access token.
        self._grab_access_token()

        # update the authentication state
        self.authstate = True

        return True

    def logout(self) -> None:
        """Clears the current TradeStation Connection state."""

        # change state to initialized so they will have to either get a
        # new access token or refresh token next time they use the API
        self._state_manager('init')

    def _grab_access_token(self) -> bool:
        """Grabs an access token.

        Overview:
        ----
        Access token handler for AuthCode Workflow. This takes the
        authorization code parsed from the auth endpoint to call the
        token endpoint and obtain an access token.

        Returns:
        ----
        (bool): `True` if grabbing the access token was successful. `False` otherwise.
        """

        # Parse the URL
        url_dict = urllib.parse.parse_qs(self.state['redirect_code'])

        # Convert the values to a list.
        url_values = list(url_dict.values())

        # Grab the Code, which is stored in a list.
        url_code = url_values[0][0]

        # define the parameters of our access token post.
        data = {
            'grant_type': 'authorization_code',
            'client_id': self.config['client_id'],
            'client_secret': self.config['client_secret'],
            'code': url_code,
            'redirect_uri': self.config['redirect_uri']
        }

        # Post the data to the token endpoint and store the response.
        token_response = requests.post(
            url=self.config['auth_endpoint'],
            data=data,
            verify=True
        )

        # Call the `_token_save` method to save the access token.
        if token_response.ok:
            self._token_save(response=token_response)
            return True
        else:
            return False

    def _silent_sso(self) -> bool:
        """Handles the silent authentication workflow.

        Overview:
        ----
        Attempt a silent authentication, by checking whether current access token
        is valid and/or attempting to refresh it. Returns True if we have successfully 
        stored a valid access token.

        Returns:
        ----
        (bool): `True` if grabbing the silent authentication was successful. `False` otherwise.
        """

        # if it's not expired we don't care.
        if self._token_validation():
            return True

        # if the current access token is expired then try and refresh access token.
        elif self.state['refresh_token'] and self._grab_refresh_token():
            return True

        # More than likely a first time login, so can't do silent authentication.
        else:
            return False

    def _grab_refresh_token(self) -> bool:
        """Refreshes the current access token if it's expired.

        Returns:
        ----
        (bool): `True` if grabbing the refresh token was successful. `False` otherwise.
        """

        # Build the parameters of our request.
        data = {
            'client_id': self.config['client_id'],
            'client_secret': self.config['client_secret'],
            'grant_type': 'refresh_token',
            'response_type': 'token',
            'refresh_token': self.state['refresh_token']
        }

        # Make a post request to the token endpoint.
        response = requests.post(
            url=self.config['auth_endpoint'],
            data=data,
            verify=True
        )

        # Save the token if the response was okay.
        if response.ok:
            self._token_save(response=response)
            return True
        else:
            return False

    def _token_save(self, response: requests.Response):
        """Saves an access token or refresh token.

        Overview:
        ----
        Parses an access token from the response of a POST request and saves it
        in the state dictionary for future use. Additionally, it will store the
        expiration time and the refresh token.

        Arguments:
        ----
        response (requests.Response): A response object received from the `token_refresh` or `_grab_access_token`
            methods.

        Returns:
        ----
        (bool): `True` if saving the token was successful. `False` otherwise.
        """

        # Parse the data.
        json_data = response.json()

        # Save the access token.
        if 'access_token' in json_data:
            self.state['access_token'] = json_data['access_token']
        else:
            self.logout()
            return False

        # If there is a refresh token then grab it.
        if 'refresh_token' in json_data:
            self.state['refresh_token'] = json_data['refresh_token']

        # Set the login state.
        self.state['logged_in'] = True

        # Store token expiration time.
        self.state['access_token_expires_in'] = json_data['expires_in']
        self.state['access_token_expires_at'] = time.time() + int(json_data['expires_in'])

        self._state_manager('save')

        return True

    def _token_seconds(self) -> int:
        """Calculates when the token will expire.

        Overview:
        ----
        Return the number of seconds until the current access token or refresh token
        will expire. The default value is access token because this is the most commonly used
        token during requests.

        Returns:
        ----
        (int): The number of seconds till expiration
        """

        # Calculate the token expire time.
        token_exp = self.state['access_token_expires_at'] - time.time()

        # if the time to expiration is less than or equal to 0, return 0.
        if not self.state['refresh_token'] or token_exp <= 0:
            token_exp = 0
        else:
            token_exp = int(token_exp)

        return token_exp

    def _token_validation(self, seconds: int = 5) -> None:
        """Validates the Access Token.

        Overview:
        ----
        Verify the current access token is valid for at least N seconds, and
        if not then attempt to refresh it. Can be used to assure a valid token
        before making a call to the TradeStation API.

        Arguments:
        ----
        seconds (int): The minimum number of seconds the token has to be valid for before
            attempting to get a refresh token.
        """

        if self._token_seconds() < seconds and self.config['refresh_enabled']:
            self._grab_refresh_token()

    def _authorize(self) -> None:
        """Authorizes the session.

        Overview:
        ----
        initializes the oAuth Workflow by creating the URL that
        allows the user to login to the TradeStation API using their credentials
        and then will parse the URL that they paste back into the terminal.
        """

        # prepare the payload to login
        if self.config['auth_version'] == 2:
            data = {
                'response_type': 'code',
                'redirect_uri': self.config['redirect_uri'],
                'client_id': self.config['client_id'],
            }
        else:
            data = {
                'response_type': 'code',
                'client_id': self.config['client_id'],
                'audience': 'https://api.tradestation.com',
                'redirect_uri': self.config['redirect_uri'],
                'scope': 'openid profile offline_access MarketData ReadAccount Trade Crypto'
            }

        # url encode the data.
        params = urllib.parse.urlencode(data)

        # build the full URL for the authentication endpoint.
        uri = 'https://api.tradestation.com/v2/authorize?' if self.config['auth_version'] == 2 else \
              'https://signin.tradestation.com/authorize?'
        url = uri + params

        # aks the user to go to the URL provided, they will be prompted to authenticate themselves.
        print('')
        print('='*80)
        print('')
        print('Please go to URL provided authorize your account: {}'.format(url))
        print('')
        print('-'*80)

        # ask the user to take the final URL after authentication and paste here so we can parse.
        my_response = input('Paste the full URL redirect here: ')

        # store the redirect URL
        self.state['redirect_code'] = my_response

    def _handle_requests(self, url: str, method: str, params: dict = None,
                         stream: bool = False, payload: dict = None):
        """[summary]

        Arguments:
        ----
        url (str): [description]

        method (str): [description]

        headers (dict): [description]

        args (dict, optional): [description]. Defaults to None.

        stream (bool, optional): [description]. Defaults to False.

        payload (dict, optional): [description]. Defaults to None.

        Raises:
        ----
        ValueError: [description]

        Returns:
        ----
        dict: [description]
        """

        # validate the token.
        self._token_validation()
        headers = self._headers()

        if method == 'get':

            # handles the non-streaming GET requests.
            if not stream:
                response = requests.get(url=url, headers=headers, params=params, verify=True)

            # handles the Streaming request.
            else:
                return requests.get(url=url, headers=headers, params=params, verify=True, stream=True)
                # for line in response.iter_lines(chunk_size=300):
                #
                #     if 'END' not in line.decode() and line.decode() != '':
                #         try:
                #             streamed_content.append(json.loads(line))
                #         except:
                #             print(line)

        elif method == 'post':

            if payload is None:
                response = requests.post(
                    url=url, headers=headers, params=params, verify=True)
            else:
                response = requests.post(
                    url=url, headers=headers, params=params, verify=True, json=payload)

        elif method == 'put':

            if payload is None:
                response = requests.put(
                    url=url, headers=headers, params=params, verify=True)
            else:
                response = requests.put(
                    url=url, headers=headers, params=params, verify=True, json=payload)

        elif method == 'delete':

            response = requests.delete(
                url=url, headers=headers, params=params, verify=True)

        else:
            raise ValueError(
                'The type of request you are making is incorrect.')

        # grab the status code
        status_code = response.status_code

        if status_code == 200:
            return response.json()

        else:
            # Error
            print('')
            print('-'*80)
            print("BAD REQUEST - STATUS CODE: {}".format(status_code))
            print("RESPONSE URL: {}".format(response.url))
            print("RESPONSE HEADERS: {}".format(response.headers))
            print("RESPONSE TEXT: {}".format(response.text))
            print('-'*80)
            print('')

    def get_bars(self, symbol: str, interval: int = 1, unit: str = "Daily", bars_back: int = 1,
                 start_date: datetime = None, session_template: str = "Default"):
        """Get quote changes for a list of symbols.

        Arguments:
        ----
        symbol (List[str]): A list of ticker symbols.

        Raises:
        ----
        ValueError: If no symbol is provided will raise an error.

        Returns:
        ----
        (dict): A dictionary containing the symbol quotes.
        """

        if bars_back > 57600:
            raise Exception(f"Bars back is too large (requested {bars_back} but limited to 57,600)")

        # define the endpoint.
        url_endpoint = self._api_endpoint(url='marketdata/barcharts/{symbol}'.format(symbol=symbol))

        params = {"interval": str(interval), "unit": unit,
                  "barsback": str(bars_back), "sessiontemplate": session_template}
        if start_date:
            params["startdate"] = start_date.isoformat()

        # grab the response.
        response = self._handle_requests(
            url=url_endpoint,
            method='get',
            params=params
        )

        return response

    def stream_bars(self, symbol: str, interval: int = 1, unit: str = "Daily",
                    bars_back: int = None, session_template: str = "USEQPost"):
        """Get quote changes for a list of symbols.

        Arguments:
        ----
        symbol (List[str]): A list of ticker symbols.

        Raises:
        ----
        ValueError: If no symbol is provided will raise an error.

        Returns:
        ----
        (dict): A dictionary containing the symbol quotes.
        """
        # define the endpoint.
        url_endpoint = self._api_endpoint(url='marketdata/stream/barcharts/{symbol}'.format(symbol=symbol))

        params = {"interval": str(interval), "unit": unit, "sessiontemplate": session_template}

        if bars_back:
            if bars_back > 57600:
                raise Exception(f"Bars back is too large (requested {bars_back} but limited to 57,600)")

            params["barsback"] = str(bars_back)

        # grab the response.
        response = self._handle_requests(
            url=url_endpoint,
            method='get',
            params=params,
            stream=True
        )

        return response

    def stream_quotes(self, symbols: List[str]):
        """Streams quote changes for a list of symbols.

        Arguments:
        ----
        symbol (List[str]): A list of ticker symbols.

        Raises:
        ----
        ValueError: If no symbol is provided will raise an error.

        Returns:
        ----
        (dict): A dictionary containing the symbol quotes.
        """

        if len(symbols) == 0:
            raise ValueError("You must pass through at least one symbol")

        symbols = ','.join(symbols)

        # define the endpoint.
        url_endpoint = self._api_endpoint(url='marketdata/stream/quotes/{symbols}'.format(symbols=symbols))

        # grab the response.
        response = self._handle_requests(
            url=url_endpoint,
            method='get',
            stream=True
        )

        return response

    # TODO implement get crypto symbol names
    def get_crypto_symbol_names(self):
        """Returns a list of crypto symbols

        Returns:
        ----
        (dict): A list of symbols.
        """

        # define the endpoint.
        url_endpoint = self._api_endpoint(url='marketdata/symbollists/cryptopairs/symbolnames')

        # grab the response.
        response = self._handle_requests(
            url=url_endpoint,
            method='get',
        )

        return response

    def get_symbol_details(self, symbols: List[str]) -> dict:
        """Returns a list of ticker symbols

        Returns:
        ----
        (dict): A list of symbols.
        """

        symbols = ','.join(symbols)

        # define the endpoint.
        url_endpoint = self._api_endpoint(url=f'marketdata/symbols/{symbols}')

        # grab the response.
        response = self._handle_requests(
            url=url_endpoint,
            method='get',
        )

        return response

    # TODO implement get interest rates
    def get_interest_rates(self):
        pass

    def get_accounts(self, user_id: str) -> dict:
        """Grabs all the accounts associated with the User.

        Arguments:
        ----
        user_id (str): The Username of the account holder.

        Returns:
        ----
        (dict): All the user accounts.
        """

        # define the endpoint.
        url_endpoint = self._api_endpoint(
            url='users/{username}/accounts'.format(username=user_id)
        )

        # define the arguments
        params = {
            'access_token': self.state['access_token']
        }

        # grab the response.
        response = self._handle_requests(
            url=url_endpoint,
            method='get',
            params=params
        )

        return response

    def get_balances(self, account_keys: List[str]) -> dict:
        """Grabs all the balances for each account provided.

        Args:
        ----
        account_keys (List[str]): A list of account numbers. Can only be a max
            of 25 account numbers

        Raises:
        ----
        ValueError: If the list is more than 25 account numbers will raise an error.

        Returns:
        ----
        dict: A list of account balances for each of the accounts.
        """

        if isinstance(account_keys, list):

            # argument validation.
            if len(account_keys) == 0:
                raise ValueError(
                    "You cannot pass through an empty list for account keys.")
            elif len(account_keys) > 25:
                raise ValueError(
                    "You cannot pass through more than 25 account keys.")

            account_keys = ','.join(account_keys)

            # define the endpoint.
            url_endpoint = self._api_endpoint(
                url='brokerage/accounts/{account_numbers}/balances'.format(
                    account_numbers=account_keys)
            )

            # define the arguments
            params = {
                'access_token': self.state['access_token']
            }

            # grab the response.
            response = self._handle_requests(
                url=url_endpoint,
                method='get',
                params=params
            )

            return response

        else:
            raise ValueError("Account Keys, must be a list object")

    def get_balances_bod(self, account_keys: List[str]) -> dict:
        """Grabs all the balances for each account provided.

        Args:
        ----
        account_keys (List[str]): A list of account numbers. Can only be a max
            of 25 account numbers

        Raises:
        ----
        ValueError: If the list is more than 25 account numbers will raise an error.

        Returns:
        ----
        dict: A list of account balances for each of the accounts.
        """

        if isinstance(account_keys, list):

            # argument validation.
            if len(account_keys) == 0:
                raise ValueError(
                    "You cannot pass through an empty list for account keys.")
            elif len(account_keys) > 25:
                raise ValueError(
                    "You cannot pass through more than 25 account keys.")

            account_keys = ','.join(account_keys)
            
            # define the endpoint.
            url_endpoint = self._api_endpoint(
                url='brokerage/accounts/{account_numbers}/bodbalances'.format(
                    account_numbers=account_keys)
            )

            # define the arguments
            params = {
                'access_token': self.state['access_token']
            }

            # grab the response.
            response = self._handle_requests(
                url=url_endpoint,
                method='get',
                params=params
            )

            return response

        else:
            raise ValueError("Account Keys, must be a list object")

    def get_historical_orders(self, account_keys: List[str], since: int) -> dict:
        """Grab all the account orders for a list of accounts.

        Overview:
        ----
        This endpoint is used to grab all the order from a list of accounts provided. Additionally,
        each account will only go back 14 days when searching for orders.

        Arguments:
        ----
        account_keys (List[str]): A list of account numbers.

        since (int): Number of days to look back, max is 14 days.

        page_size (int): The page size.

        page_number (int, optional): The page number to return if more than one. Defaults to 0.

        Raises:
        ----
        ValueError: If the list is more than 25 account numbers will raise an error.

        Returns:
        ----
        dict: A list of account balances for each of the accounts.
        """

        if isinstance(account_keys, list):

            # argument validation, account keys.
            if len(account_keys) == 0:
                raise ValueError(
                    "You cannot pass through an empty list for account keys.")
            elif len(account_keys) > 25:
                raise ValueError(
                    "You cannot pass through more than 25 account keys.")

            account_keys = ','.join(account_keys)

            # argument validation, SINCE
            if since:
                if since > 90:
                    raise ValueError(
                        "You can't get orders older than 14 days old.")
                elif since <= 0:
                    raise ValueError(
                        "You can't specify since as a 0 or a negative number.")

                today = date.today()
                today = date(year=today.year, month=today.month, day=today.day) - timedelta(days=since)
                date_format = today.strftime("%Y/%m/%d")

            else:
                date_format = None

            params = {
                'access_token': self.state['access_token'],
                'since': date_format
            }

            # define the endpoint.
            url_endpoint = self._api_endpoint(
                url='brokerage/accounts/{account_numbers}/historicalorders'.format(
                    account_numbers=account_keys)
            )

            # grab the response.
            response = self._handle_requests(
                url=url_endpoint,
                method='get',
                params=params
            )

            return response

        else:
            raise ValueError("Account Keys, must be a list object")

    def get_orders(self, account_keys: List[str]) -> dict:
        """Grab all the account orders for a list of accounts.

        Overview:
        ----
        This endpoint is used to grab all the order from a list of accounts provided. Additionally,
        each account will only go back 14 days when searching for orders.

        Arguments:
        ----
        account_keys (List[str]): A list of account numbers.

        since (int): Number of days to look back, max is 14 days.

        page_size (int): The page size.

        page_number (int, optional): The page number to return if more than one. Defaults to 0.

        Raises:
        ----
        ValueError: If the list is more than 25 account numbers will raise an error.

        Returns:
        ----
        dict: A list of account balances for each of the accounts.
        """

        if isinstance(account_keys, list):

            # argument validation, account keys.
            if len(account_keys) == 0:
                raise ValueError(
                    "You cannot pass through an empty list for account keys.")
            elif len(account_keys) > 25:
                raise ValueError(
                    "You cannot pass through more than 25 account keys.")

            account_keys = ','.join(account_keys)
            params = {
                'access_token': self.state['access_token']
            }

            # define the endpoint.
            url_endpoint = self._api_endpoint(
                url='brokerage/accounts/{account_numbers}/orders'.format(
                    account_numbers=account_keys)
            )

            # grab the response.
            response = self._handle_requests(
                url=url_endpoint,
                method='get',
                params=params
            )

            return response

        else:
            raise ValueError("Account Keys, must be a list object")

    def get_positions(self, account_keys: List[str]) -> dict:
        """Grabs all the account positions.

        Arguments:
        ----
        account_keys (List[str]): A list of account numbers..

        symbols (List[str]): A list of ticker symbols, you want to return.

        Raises:
        ----
        ValueError: If the list is more than 25 account numbers will raise an error.

        Returns:
        ----
        dict: A list of account balances for each of the accounts.
        """

        if isinstance(account_keys, list):

            # argument validation, account keys.
            if len(account_keys) == 0:
                raise ValueError(
                    "You cannot pass through an empty list for account keys")
            elif len(account_keys) > 25:
                raise ValueError(
                    "You cannot pass through more than 25 account keys")
            
            account_keys = ','.join(account_keys)
            params = {
                'access_token': self.state['access_token']
            }

            # define the endpoint.
            url_endpoint = self._api_endpoint(
                url='brokerage/accounts/{account_numbers}/positions'.format(account_numbers=account_keys)
            )

            # grab the response.
            response = self._handle_requests(
                url=url_endpoint,
                method='get',
                params=params
            )

            return response

        else:
            raise ValueError("Account Keys, must be a list object")

    # TODO implement get wallets
    def get_wallets(self, account_keys: List[str]) -> dict:
        pass

    # TODO implement stream wallets
    def stream_wallets(self, account_keys: List[str]) -> dict:
        pass

    def confirm_order(self, account_key: str, symbol: str, trade_action: str, quantity: str,
                      order_type: str, time_in_force: str, advanced_options: dict = None,
                      limit_price: int = None, stop_price: int = None) -> dict:
        """Submit an order.

        Arguments:
        ----
        order (dict): A dictionary for order.

        Returns:
        ----
        dict: A confirmation of the order.
        """

        # define the endpoint.
        url_endpoint = self._api_endpoint(url='orderexecution/orderconfirm')
        order = {"AccountID": account_key, "Symbol": symbol, "TradeAction": trade_action,
                 "Quantity": quantity, "OrderType": order_type, "TimeInForce": time_in_force}

        if advanced_options:
            order["AdvancedOptions"] = advanced_options
        if limit_price:
            order["LimitPrice"] = limit_price
        if stop_price:
            order["StopPrice"] = stop_price

        # grab the response.
        response = self._handle_requests(
            url=url_endpoint,
            method='post',
            payload=order
        )

        return response
    
    def confirm_group_order(self, orders: List[Dict]) -> dict:
        # TODO implement confirm group order
        pass
    #     """Confirm a list of orders.
    # 
    #     Arguments:
    #     ----
    #     orders (List[dict]): A list of orders to confirm.
    # 
    #     Returns:
    #     ----
    #     dict: A confirmation for all the orders.
    #     """
    # 
    #     # define the endpoint.
    #     url_endpoint = self._api_endpoint(url='orders/groups/confirm')
    # 
    #     # define the arguments.
    #     params = {
    #         'access_token': self.state['access_token']
    #     }
    # 
    #     # grab the response.
    #     response = self._handle_requests(
    #         url=url_endpoint,
    #         method='post',
    #         params=params,
    #         payload=orders
    #     )
    # 
    #     return response

    def place_order(self, account_key: str, symbol: str, trade_action: str, quantity: int,
                    order_type: str, duration: str, **kwargs) -> dict:
        """Submit an order.

        Arguments:
        ----
        order (dict): A dictionary for order.

        Returns:
        ----
        dict: A confirmation of the order.
        """

        # define the endpoint.
        url_endpoint = self._api_endpoint(url='orderexecution/orders')
        order = {"AccountID": account_key, "Symbol": symbol, "TradeAction": trade_action,
                 "Quantity": str(quantity), "OrderType": order_type, "TimeInForce": {"Duration": duration}}
        
        order.update(kwargs)

        # grab the response.
        response = self._handle_requests(
            url=url_endpoint,
            method='post',
            payload=order
        )

        return response

    def place_group_order(self, orders: List[Dict]) -> dict:
        # TODO implement place group order
        pass
    #     """Submit a list of orders.
    # 
    #     Arguments:
    #     ----
    #     orders (List[dict]): A list of orders to submit.
    # 
    #     Returns:
    #     ----
    #     dict: A confirmation for all the orders.
    #     """
    # 
    #     # define the endpoint.
    #     url_endpoint = self._api_endpoint(url='orders/groups')
    # 
    #     # define the arguments.
    #     params = {
    #         'access_token': self.state['access_token']
    #     }
    # 
    #     # grab the response.
    #     response = self._handle_requests(
    #         url=url_endpoint,
    #         method='post',
    #         params=params,
    #         payload=orders
    #     )
    # 
    #     return response

    def replace_order(self, order_id: str, new_order: dict) -> dict:
        # TODO implement replace order
        pass
    #     """Replace an order.
    # 
    #     Arguments:
    #     ----
    #     order_id (str): An order id.
    # 
    #     order (dict): A dictionary for order.
    # 
    #     Returns:
    #     ----
    #     dict: A confirmation of the replaced order.
    #     """
    # 
    #     # define the endpoint.
    #     url_endpoint = self._api_endpoint(
    #         url='orders/{order_id}'.format(order_id=order_id)
    #     )
    # 
    #     # define the arguments.
    #     params = {
    #         'access_token': self.state['access_token']
    #     }
    # 
    #     # grab the response.
    #     response = self._handle_requests(
    #         url=url_endpoint,
    #         method='put',
    #         params=params,
    #         payload=new_order
    #     )
    # 
    #     return response

    def cancel_order(self, order_id: str) -> dict:
        # TODO implement cancel order
        pass
    #     """Cancel an order.
    # 
    #     Arguments:
    #     ----
    #     order_id (str): An order id.
    # 
    #     Returns:
    #     ----
    #     dict: A confirmation of the cancel order.
    #     """
    # 
    #     # define the endpoint.
    #     url_endpoint = self._api_endpoint(
    #         url='orders/{order_id}'.format(order_id=order_id))
    # 
    #     # define the arguments.
    #     params = {
    #         'access_token': self.state['access_token']
    #     }
    # 
    #     # grab the response.
    #     response = self._handle_requests(
    #         url=url_endpoint,
    #         method='delete',
    #         params=params
    #     )
    # 
    #     return response

    def get_activation_triggers(self):
        # TODO implement get activation triggers
        pass
    #     """Grabs all the Activation Triggers.
    # 
    #     Returns:
    #     ----
    #     (dict): A dictionary resource with all the activation triggers.
    #     """
    # 
    #     # define the endpoint.
    #     url_endpoint = self._api_endpoint(
    #         url='orderexecution/activationtriggers'
    #     )
    # 
    #     # define the arguments.
    #     params = {
    #         'access_token': self.state['access_token']
    #     }
    # 
    #     # grab the response.
    #     response = self._handle_requests(
    #         url=url_endpoint,
    #         method='get',
    #         params=params
    #     )
    # 
    #     return response

    def get_routes(self) -> dict:
        # TODO implement get routes
        pass
