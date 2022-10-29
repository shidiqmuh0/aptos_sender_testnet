from multiprocessing.dummy import Pool
from os import getenv
from sys import stderr
from time import sleep

from aptos_sdk.account import Account
from aptos_sdk.client import RestClient
from loguru import logger

logger.remove()
logger.add(stderr, format="<white>{time:HH:mm:ss}</white>"
                          " | <level>{level: <8}</level>"
                          " | <cyan>{line}</cyan>"
                          " - <white>{message}</white>")

NODE_URL = getenv("APTOS_NODE_URL", "https://fullnode.testnet.aptoslabs.com/v1")
REST_CLIENT = RestClient(NODE_URL)


class App:
    def claim_tokens(self,
                     private_key: str) -> None:
        try:
            current_account = Account.load_key(key=private_key)

        except ValueError:
            logger.error(f'{private_key} | Invalid Private Key')
            return

        while True:
            try:
                account_balance = int(REST_CLIENT.account_balance(account_address=str(current_account.address())))
                gas_price = 54100

                if account_balance <= gas_price:
                    logger.info(f'{private_key} | Small balance: {account_balance / 100000000}')
                    return

                tx_hash = REST_CLIENT.transfer(sender=current_account,
                                               recipient=main_wallet,
                                               amount=account_balance - gas_price)

                logger.success(f'{private_key} | {tx_hash}')

            except Exception as error:
                logger.error(f'{private_key} | {error}')

                if '{"message":"' in str(error):
                    return

            else:
                return

    def send_tokens(self,
                    wallet: str) -> None:
        account_balance = None

        try:
            current_account = Account.load_key(key=main_private_key)

        except ValueError:
            logger.error(f'{wallet} | Invalid Private Key')
            return

        while True:
            try:
                account_balance = int(REST_CLIENT.account_balance(account_address=str(current_account.address())))
                gas_price = 54100

                if account_balance <= to_wallets_value + gas_price:
                    logger.info(f'{wallet} | Small balance: {account_balance / 100000000}')
                    return

                tx_hash = REST_CLIENT.transfer(sender=current_account,
                                               recipient=wallet,
                                               amount=to_wallets_value)

                logger.success(f'{wallet} | {tx_hash}')

            except Exception as error:
                logger.error(f'{wallet} | {error}')

                if 'INSUFFICIENT_BALANCE_FOR_TRANSACTION_FEE' in str(error):
                    if account_balance:
                        logger.error(f'{wallet} | Small balance: {account_balance / 100000000}')

                    else:
                        logger.error(f'{wallet} | Small balance')

                    return

                elif 'SEQUENCE_NUMBER_TOO_OLD' or '"Transaction already in mempool with a different payload"' in str(error):
                    sleep(1)
                    continue

                elif '{"message":"' in str(error):
                    return

            else:
                return


def send_to_one_wrapper(private_key: str):
    App().claim_tokens(private_key=private_key)


def send_to_other_wrapper(wallet: str):
    App().send_tokens(wallet=wallet)


if __name__ == '__main__':
    threads = int(input('Enter the number of threads: '))
    user_action = int(input('1. Collect APT for one wallet\n'
                            '2. Scatter APT from one wallet\n'
                            'Enter your action: '))

    if user_action == 1:
        main_wallet = input('Enter main wallet: ')

        with open('private_keys.txt', 'r', encoding='utf-8-sig') as file:
            private_keys = [row.strip() for row in file]

        logger.info(f'Uploaded successfully {len(private_keys)} private key\'s')

        with Pool(processes=threads) as executor:
            executor.map(send_to_one_wrapper, private_keys)

    elif user_action == 2:
        main_private_key = input('Enter the Private Key of the main wallet: ')
        to_wallets_value = int(float(input('Enter the amount of tokens to send (float): '))
                               * 100000000)

        with open('wallets.txt', 'r', encoding='utf-8-sig') as file:
            wallets = [row.strip() for row in file]

        logger.info(f'Successfully uploaded {len(wallets)} wallet\'s')

        with Pool(processes=threads) as executor:
            executor.map(send_to_other_wrapper, wallets)
