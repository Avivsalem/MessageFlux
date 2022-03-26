import asyncio
from abc import ABCMeta, abstractmethod
from asyncio import Event
from typing import Optional, Tuple, List, Union, TYPE_CHECKING

from baseservice.iodevices.base.common import Message, DeviceHeaders
if TYPE_CHECKING:
    from baseservice.iodevices.async_devices.async_input_devices import AsyncInputDevice


class AsyncInputTransaction(metaclass=ABCMeta):
    """
    this is the base class for input transaction objects.

    after reading a message from the device (using 'with_transaction=True') the reader should commit/rollback
    the transaction, to signal the device if the message is done processing or not
    """

    def __init__(self, device: Optional['AsyncInputDevice'] = None):
        """

        :param device: the input device that returned that transaction
        """
        self._device: Optional['AsyncInputDevice'] = device
        self._finished: Event = Event()

    @property
    def device(self) -> Optional['AsyncInputDevice']:
        """
        :return: the input device that returned that transaction
        """
        return self._device

    @property
    def finished(self) -> bool:
        """
        :return: 'True' if the transaction committed/rolledback, 'False' otherwise
        """
        return self._finished.is_set()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if (exc_type is None) and (exc_val is None) and (exc_tb is None):
            await self.commit()  # no exception, commit the transaction
        else:
            await self.rollback()  # exception, rollback the transaction

    async def wait_for_finish(self, timeout: Optional[float] = None) -> bool:
        """
        waits for the transaction to finish (commit/rollback).
        it timeout is not None, after timeout seconds, it will return self.finished

        :param timeout: the timeout (in seconds) to wait for the transaction to finish

        :return: the value of self.finished
        """
        try:
            await asyncio.wait_for(self._finished.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            pass
        return self.finished

    async def commit(self):
        """
        commits the transaction
        """
        if self.finished:
            return
        await self._commit()
        self._finished.set()

    async def rollback(self):
        """
        rolls back the transaction
        """
        if self.finished:
            return
        await self._rollback()
        self._finished.set()

    @abstractmethod
    async def _commit(self):
        """
        this method should be implemented by child classes to actually perform the commit
        """
        pass

    @abstractmethod
    async def _rollback(self):
        """
        this method should be implemented by child classes to actually perform the rollback
        """
        pass


class AsyncInputTransactionScope(AsyncInputTransaction):
    """
    a helper class for reading several messages inside a transaction scope.
    """

    def __init__(self, device: Optional['AsyncInputDevice'] = None, with_transaction: bool = True):
        """

        :param device: the input device to read the messages from
        :param with_transaction: 'True' if we should actually use transactions, 'False' otherwise
        """
        super().__init__(device)
        self._with_transaction = with_transaction
        self._transactions: List[AsyncInputTransaction] = []

    async def read_message(self, timeout: Optional[float] = 0) -> Union[Tuple[Message, DeviceHeaders], Tuple[None, None]]:
        message, device_headers, transaction = await self.device.read_message(timeout=timeout,
                                                                        with_transaction=self._with_transaction)

        if transaction is not None:
            self._transactions.append(transaction)

        return message, device_headers

    async def _commit(self):
        """
        commits all the transactions in scope
        """
        for transaction in self._transactions:
            await transaction.commit()
        self._transactions.clear()

    async def _rollback(self):
        """
        rolls back all the transaction in scope
        """
        for transaction in self._transactions:
            await transaction.rollback()
        self._transactions.clear()


class AsyncNULLTransaction(AsyncInputTransaction):
    """
    a transaction object that does nothing. used as placeholder for some places
    """

    async def _commit(self):
        pass

    async def _rollback(self):
        pass


ASYNC_NULL_TRANSACTION = AsyncNULLTransaction()
