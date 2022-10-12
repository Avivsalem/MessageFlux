import logging

from messageflux.iodevices.base import InputTransaction, InputDevice, ReadResult, InputDeviceManager
from messageflux.iodevices.message_store_device_wrapper.message_store_base import MessageStoreBase
from messageflux.iodevices.message_store_device_wrapper.message_store_transformer_base import \
    _MessageStoreTransformerBase
from messageflux.iodevices.transformer_device_wrapper import InputTransformerBase, TransformerInputDeviceManager
from messageflux.iodevices.transformer_device_wrapper.transformer_input_device import TransformerInputDevice


class MessageStoreInputTransactionWrapper(InputTransaction):
    """
    represents an Message Store wrapper transaction
    """

    _logger = logging.getLogger(__name__)

    def __init__(self,
                 device: InputDevice,
                 inner_transaction: InputTransaction,
                 message_store: MessageStoreBase,
                 key: str,
                 delete_on_commit: bool = True):
        """
        represents an message store wrapper transaction

        :param device: the input device that returned that transaction
        :param inner_transaction: the actual transaction
        :param MessageStoreBase message_store: the message store that stores this item
        :param str key: the key to this message
        :param delete_on_commit: Whether or not we should delete the stored buffer after committing a message from the
                                 relevant input device
        """
        super(MessageStoreInputTransactionWrapper, self).__init__(device)
        self._inner_transaction = inner_transaction
        self._delete_on_commit = delete_on_commit
        self._message_store = message_store
        self._key = key

    def _commit(self):
        """
        commits the transaction
        """
        self._inner_transaction.commit()
        if self._delete_on_commit:
            try:
                self._message_store.delete_message(key=self._key)
            except Exception:
                self._logger.warning(f'Error deleting key {self._key}', exc_info=True)

    def _rollback(self):
        """
        rolls back the transaction
        """
        self._inner_transaction.rollback()


class MessageStoreInputTransformer(_MessageStoreTransformerBase, InputTransformerBase):
    """
    a transformer class that converts key to message from the message store
    """

    def __init__(self,
                 message_store: MessageStoreBase,
                 delete_on_commit: bool = True):
        """
        :param message_store: the message store to use
        :param delete_on_commit: should we the delete the message from the message store on transaction commit?
        """
        super(MessageStoreInputTransformer, self).__init__(message_store=message_store)
        self._delete_on_commit = delete_on_commit

    def transform_incoming_message(self, input_device: TransformerInputDevice, read_result: ReadResult) -> ReadResult:
        """
        Transform the message that was received from the underlying device.
        in our case, converts the key received from the device, to the actual message from the message store
        in case the message received is not a key, doesn't do anything

        :param input_device: the input device that the transformer runs on
        :param read_result: the original ReadResult received from the underlying device
        :return: the transformed ReadResult
        """
        try:
            if not self.is_key(read_result.message.stream):
                return read_result

            key = self.deserialize_key(read_result.message.bytes)
            store_message_bundle = self._message_store.read_message(key)

            wrapped_transaction = MessageStoreInputTransactionWrapper(input_device,
                                                                      inner_transaction=read_result.transaction,
                                                                      message_store=self._message_store,
                                                                      key=key,
                                                                      delete_on_commit=self._delete_on_commit)

            store_message_bundle.message.headers.update(read_result.message.headers)
            store_message_bundle.device_headers.update(read_result.device_headers)

        except Exception:
            read_result.rollback()
            raise

        return ReadResult(message=store_message_bundle.message,
                          device_headers=store_message_bundle.device_headers,
                          transaction=wrapped_transaction)


class MessageStoreInputDeviceManagerWrapper(TransformerInputDeviceManager):
    """
    This class is used to wrap InputDeviceManager with message store functionality
    """

    def __init__(self,
                 inner_manager: InputDeviceManager,
                 message_store: MessageStoreBase,
                 delete_on_commit: bool = True):
        """
        This class is used to wrap IODeviceManager with message store functionality

        :param inner_manager: the actual manager to wrap
        :param message_store: the message store to use to store files
        :param delete_on_commit: When reading a message that was stored in the relevant message store, should we delete
                                 the stored message when committing the original input message? Sometimes we might want
                                 to leave the stored message in order to avoid conflicts between systems that read
                                 duplications of the same message.
        """
        transformer = MessageStoreInputTransformer(message_store=message_store,
                                                   delete_on_commit=delete_on_commit)
        super(MessageStoreInputDeviceManagerWrapper, self).__init__(inner_manager, transformer)
