from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from .rabbitmq_channel import RabbitMQChannel


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.handler_connection = RabbitMQChannel(host)
        self.queue_name = self.handler_connection.setup_queue(queue_name)

    def start_consuming(self, on_message_callback):
        self.handler_connection.consume(self.queue_name, on_message_callback)

    def stop_consuming(self):
        self.handler_connection.stop_consuming()

    def send(self, message):
        self.handler_connection.publish('', self.queue_name, message)

    def close(self):
        self.handler_connection.close()

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):

    def __init__(self, host, exchange_name, routing_keys):
        self.handler_connection = RabbitMQChannel(host, prefetch_count=0)
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.queue_name = self.handler_connection.subscribe_to_exchange(exchange_name, routing_keys)

    def start_consuming(self, on_message_callback):
        self.handler_connection.consume(self.queue_name, on_message_callback)

    def stop_consuming(self):
        self.handler_connection.stop_consuming()

    def send(self, message):
        for key in self.routing_keys:
            self.handler_connection.publish(self.exchange_name, key, message)

    def close(self):
        self.handler_connection.close()
