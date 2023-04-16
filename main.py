import asyncio
from json import load
from multiprocessing.dummy import Pool
from sys import stderr

import aiohttp
from loguru import logger
from web3 import Web3
from web3.auto import w3
from web3.eth import AsyncEth

logger.remove()
logger.add(stderr, format="<white>{time:HH:mm:ss}</white>"
                          " | <level>{level: <8}</level>"
                          " | <cyan>{line}</cyan>"
                          " - <white>{message}</white>")


def format_keys(value: str) -> str:
    if value.startswith('0x'):
        return value

    return f'0x{value}'


async def get_address(private_key: str) -> str:
    while True:
        try:
            address = w3.to_checksum_address(w3.eth.account.from_key(private_key).address)

            return address

        except Exception as error:
            logger.error(f'{private_key} | Ошибка при получении адреса: {error}')


async def get_gwei(address: str) -> int:
    while True:
        try:
            current_gwei = w3.from_wei(await provider.eth.gas_price, 'gwei')

            return current_gwei

        except Exception as error:
            logger.error(f'{address} | Ошибка при получении среднего GWEI: {error}')


class ClaimMain:
    @staticmethod
    async def get_tx_data(address: str) -> tuple[str, str]:
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post('https://api.arbdoge.ai/arb/eligibility/claim',
                                            data={
                                                'address': address
                                            }) as r:
                        return (await r.json())['data']['nonce'], (await r.json())['data']['signature']

            except Exception as error:
                logger.error(f'{address} | Ошибка при получении signature и nonce: {error}')

    @staticmethod
    async def send_tx(private_key: str,
                      address: str,
                      site_nonce: str,
                      site_signature: str) -> bool:
        chain_id = await provider.eth.chain_id
        nonce = await provider.eth.get_transaction_count(address)

        if GWEI_CLAIM == 'auto':
            current_gwei = await get_gwei(address=address)

        else:
            current_gwei = GWEI_CLAIM

        if GAS_LIMIT_CLAIM == 'auto':
            build_tx_data = {
                'chainId': chain_id,
                'gasPrice': w3.to_wei(current_gwei, 'gwei'),
                'from': address,
                'nonce': nonce,
                'value': 0
            }

            current_gas_limit = await claim_contract.functions.claim(int(site_nonce),
                                                                     site_signature,
                                                                     '0xDEADf12DE9A24b47Da0a43E1bA70B8972F5296F2') \
                .estimate_gas(build_tx_data)

        else:
            current_gas_limit = GAS_LIMIT_CLAIM

        build_tx_data = {
            'gas': current_gas_limit,
            'chainId': chain_id,
            'gasPrice': w3.to_wei(current_gwei, 'gwei'),
            'from': address,
            'nonce': nonce,
            'value': 0
        }

        transaction = await claim_contract.functions.claim(int(site_nonce),
                                                           site_signature,
                                                           '0xDEADf12DE9A24b47Da0a43E1bA70B8972F5296F2') \
            .build_transaction(build_tx_data)

        signed_txn = provider.eth.account.sign_transaction(transaction_dict=transaction,
                                                           private_key=private_key)

        while True:
            try:
                await provider.eth.send_raw_transaction(signed_txn.rawTransaction)

            except Exception as error:
                logger.error(f'{address} | Ошибка при отправке TX: {error}')

            else:
                break

        tx_hash = w3.to_hex(w3.keccak(signed_txn.rawTransaction))
        logger.info(f'{address} | {tx_hash}')

        while True:
            try:
                tx_receipt = await provider.eth.wait_for_transaction_receipt(tx_hash)

                if tx_receipt['status'] == 1:
                    logger.success(f'{address} | Claimed: {tx_hash}')
                    return True

                else:
                    logger.error(f'{address} | Not Claimed: {tx_hash}')
                    return False

            except Exception as error:
                logger.error(f'{address} | Ошибка при ожидании статуса транзакции: {error}')

    async def main_work(self,
                        private_key: str) -> None:
        address = await get_address(private_key=private_key)
        site_nonce, site_signature = await self.get_tx_data(address=address)

        _claim_status = await self.send_tx(private_key=private_key,
                                           address=address,
                                           site_nonce=site_nonce,
                                           site_signature=site_signature)


def claim_wrapper(private_key: str) -> None:
    try:
        asyncio.run(ClaimMainObj.main_work(private_key=private_key))

    except Exception as error:
        logger.error(f'{private_key} | Unexpected Error: {error}')

        with open('claim_errors.txt', 'a', encoding='utf-8-sig') as f:
            f.write(f'{private_key}\n')


class TransferMain:
    @staticmethod
    async def get_token_balance(address: str) -> float:
        while True:
            try:
                balance = w3.from_wei(await token_contract.functions.balanceOf(address).call(), 'ether')

                return balance

            except Exception as error:
                logger.error(f'{address} | Ошибка при получении баланса токенов: {error}')

    @staticmethod
    async def send_tx(private_key: str,
                      address: str,
                      value: float):
        chain_id = await provider.eth.chain_id
        nonce = await provider.eth.get_transaction_count(address)

        if GWEI_TRANSFER == 'auto':
            current_gwei = await get_gwei(address=address)

        else:
            current_gwei = GWEI_TRANSFER

        if GAS_LIMIT_TRANSFER == 'auto':
            build_tx_data = {
                'chainId': chain_id,
                'gasPrice': w3.to_wei(current_gwei, 'gwei'),
                'from': address,
                'nonce': nonce,
                'value': 0
            }

            current_gas_limit = await token_contract.functions.transfer(TRANSFER_TO_ADDRESS,
                                                                        w3.to_wei(value,
                                                                                  'ether')) \
                .estimate_gas(build_tx_data)

        else:
            current_gas_limit = GAS_LIMIT_TRANSFER

        build_tx_data = {
            'gas': current_gas_limit,
            'chainId': chain_id,
            'gasPrice': w3.to_wei(current_gwei, 'gwei'),
            'from': address,
            'nonce': nonce,
            'value': 0
        }

        transaction = await token_contract.functions.transfer(TRANSFER_TO_ADDRESS,
                                                              w3.to_wei(value,
                                                                        'ether')) \
            .build_transaction(build_tx_data)

        signed_txn = provider.eth.account.sign_transaction(transaction_dict=transaction,
                                                           private_key=private_key)

        while True:
            try:
                await provider.eth.send_raw_transaction(signed_txn.rawTransaction)

            except Exception as error:
                logger.error(f'{address} | Ошибка при отправке TX: {error}')

            else:
                break

        tx_hash = w3.to_hex(w3.keccak(signed_txn.rawTransaction))

        logger.info(f'{address} | {tx_hash}')

        while True:
            try:
                tx_receipt = await provider.eth.wait_for_transaction_receipt(tx_hash)

                if tx_receipt['status'] == 1:
                    logger.success(f'{address} | Transferred: {tx_hash}')
                    return True

                else:
                    logger.error(f'{address} | Not Transferred: {tx_hash}')
                    return False

            except Exception as error:
                logger.error(f'{address} | Ошибка при ожидании статуса транзакции: {error}')

    async def main_work(self,
                        private_key: str) -> None:
        address = await get_address(private_key=private_key)
        token_balance = await self.get_token_balance(address=address)

        if token_balance > 0:
            _transfer_result = await self.send_tx(private_key=private_key,
                                                  address=address,
                                                  value=token_balance)


def transfer_wrapper(private_key: str) -> None:
    try:
        asyncio.run(TransferMainObj.main_work(private_key=private_key))

    except Exception as error:
        logger.error(f'{private_key} | Unexpected Error: {error}')

        with open('transfer_errors.txt', 'a', encoding='utf-8-sig') as f:
            f.write(f'{private_key}\n')


if __name__ == '__main__':
    ClaimMainObj = ClaimMain()
    TransferMainObj = TransferMain()

    with open('claim_abi.json', 'r', encoding='utf-8-sig') as file:
        claim_abi = file.read().strip().replace('\n', '').replace(' ', '')

    with open('token_abi.json', 'r', encoding='utf-8-sig') as file:
        token_abi = file.read().strip().replace('\n', '').replace(' ', '')

    with open('settings.json', 'r', encoding='utf-8-sig') as file:
        settings_json = load(file)

    RPC_URL: str = settings_json['rpc_url']
    CLAIM_CONTRACT_ADDRESS: str = w3.to_checksum_address(value=settings_json['claim_contract_address'])
    TOKEN_CONTRACT_ADDRESS: str = w3.to_checksum_address(value=settings_json['token_contract_address'])
    TRANSFER_TO_ADDRESS: str = settings_json['transfer_to_address']
    GAS_LIMIT_CLAIM: int | str = settings_json['gas_limit_claim']
    GWEI_CLAIM: float | str = settings_json['gwei_claim']
    GAS_LIMIT_TRANSFER: int | str = settings_json['gas_limit_transfer']
    GWEI_TRANSFER: int | str = settings_json['gwei_transfer']

    values = [(GAS_LIMIT_CLAIM, int), (GWEI_CLAIM, float), (GAS_LIMIT_TRANSFER, int), (GWEI_TRANSFER, float)]

    for value, func in values:
        match value:
            case int(value):
                value = func(value)
            case str(value):
                pass

    with open('accounts.txt', 'r', encoding='utf-8-sig') as file:
        accounts_list = [format_keys(value=row.strip()) for row in file]

    logger.info(f'Успешно загружено {len(accounts_list)} аккаунтов')
    user_action = int(input('1. Claim\n'
                            '2. Transfer\n'
                            'Выберите ваше действие: '))
    threads = int(input('\nThreads: '))
    print('')

    provider = Web3(Web3.AsyncHTTPProvider(RPC_URL), modules={'eth': (AsyncEth,)}, middlewares=[])
    claim_contract = provider.eth.contract(address=CLAIM_CONTRACT_ADDRESS, abi=claim_abi)
    token_contract = provider.eth.contract(address=TOKEN_CONTRACT_ADDRESS, abi=token_abi)

    if user_action == 1:
        with Pool(processes=threads) as executor:
            executor.map(claim_wrapper, accounts_list)

    elif user_action == 2:
        TRANSFER_TO_ADDRESS = w3.to_checksum_address(value=TRANSFER_TO_ADDRESS)

        with Pool(processes=threads) as executor:
            executor.map(transfer_wrapper, accounts_list)

    logger.success('Работа успешно завершена')
    input('\nPress Enter To Exit..')
