from messageflux.iodevices.base import OutputDeviceException, OutputDeviceManager
from messageflux.iodevices.base.common import MessageBundle, Message
from messageflux.iodevices.message_store_device_wrapper.message_store_base import MessageStoreBase
from messageflux.iodevices.message_store_device_wrapper.message_store_transformer_base import \
    _MessageStoreTransformerBase
from messageflux.iodevices.transformer_device_wrapper import OutputTransformerBase
from messageflux.iodevices.transformer_device_wrapper.transformer_output_device import TransformerOutputDevice, \
    TransformerOutputDeviceManager


class MessageStoreOutputTransformer(_MessageStoreTransformerBase, OutputTransformerBase):
    """
    a transformer class that converts key to message from the message store
    """

    ORIGINAL_MESSAGE_SIZE_HEADER = '__ORIGINAL_MESSAGE_SIZE_HEADER__'

    def __init__(self,
                 message_store: MessageStoreBase,
                 size_threshold: int = -1):
        """
        :param message_store: the message store to use
        :param size_threshold: the minimum message size, the only above we store the message in message store.
        """
        super().__init__(message_store=message_store)
        self._size_threshold = size_threshold

    def transform_outgoing_message(self,
                                   output_device: TransformerOutputDevice,
                                   message_bundle: MessageBundle) -> MessageBundle:
        """
        Transform the message before it is sent to the underlying device.
        in our case, stores the message in message store, and returns the key as the output message

        if the message size is under size_threshold, nothing is done

        :param output_device: the output device that the transformer runs on
        :param message_bundle: the message bundle to transform
        :return: the transformed message bundle to send through the underlying device
        """

        stream_size = message_bundle.message.stream.seek(0, 2)
        message_bundle.message.stream.seek(0)

        if stream_size > self._size_threshold:
            # send using massage store
            result_message = self._store_in_message_store(output_device, message_bundle)
            result_message.headers[self.ORIGINAL_MESSAGE_SIZE_HEADER] = stream_size
            return MessageBundle(result_message, message_bundle.device_headers)
        else:
            # send without message store
            return message_bundle

    def _store_in_message_store(self,
                                output_device: TransformerOutputDevice,
                                message_bundle: MessageBundle) -> Message:
        """
        stores the message in the message store

        :param output_device: the output device that the transformer runs on
        :param message_bundle: the message bundle to store

        :return: the message containing the key to the message store
        """
        try:
            # save to object storage and get the key
            key = self._message_store.put_message(output_device.name, message_bundle)
        except Exception as ex:
            raise OutputDeviceException('Error putting item into message store') from ex
        try:
            # send the key instead of the buffer over the inner device
            data = self.serialize_key(key)
            return Message(data, message_bundle.message.headers)
        except Exception:
            try:
                # delete from the object storage
                self._message_store.delete_message(key)
            except Exception as del_ex:
                raise OutputDeviceException(
                    'Error deleting item from message store') from del_ex  # ex will be in __context__
            raise


class MessageStoreOutputDeviceManagerWrapper(TransformerOutputDeviceManager):
    """
    This class is used to wrap IODeviceManager with message store functionality
    """

    def __init__(self,
                 inner_manager: OutputDeviceManager,
                 message_store: MessageStoreBase,
                 size_threshold: int = -1):
        """
        This class is used to wrap IODeviceManager with message store functionality

        :param inner_manager: the actual manager to wrap
        :param MessageStoreBase message_store: the message store to use to store files
        :param size_threshold: only use message store if the size of the product (in bytes) is larger than this.
                               set to -1 to always use message store (default: -1).

        """
        transformer = MessageStoreOutputTransformer(message_store=message_store,
                                                    size_threshold=size_threshold)

        super(MessageStoreOutputDeviceManagerWrapper, self).__init__(inner_manager, transformer)
        self.size_threshold = size_threshold
