import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError, MessageMiddlewareCloseError


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=queue_name, durable=True)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"conexion failed, host: {host}, error: {e}")
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(f"error: {e}")
        self.queue_name = queue_name
        self.consuming = False
        self.channel.basic_qos(prefetch_count=1)

    def start_consuming(self, on_message_callback):
        _start_consuming(self, on_message_callback)

    def stop_consuming(self):
        _stop_consuming(self)

    def send(self, message):
        _publish(self, '', self.queue_name, message)

    def close(self):
        _close(self)

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):

    def __init__(self, host, exchange_name, routing_keys):
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            self.channel = self.connection.channel()
            self.channel.exchange_declare(exchange=exchange_name, exchange_type='direct')
            result = self.channel.queue_declare(queue='', exclusive=True)
            self.channel.basic_qos()
            for key in routing_keys:
                self.channel.queue_bind(exchange=exchange_name,
                                   queue=result.method.queue,
                                   routing_key=key)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"conexion failed, host: {host}, error: {e}")
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(f"error: {e}")
        self.queue_name = result.method.queue
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.consuming = False


    def start_consuming(self, on_message_callback):
            _start_consuming(self, on_message_callback)

    def stop_consuming(self):
        _stop_consuming(self)

    def send(self, message):
        for key in self.routing_keys:
            _publish(self, self.exchange_name, key, message)


    def close(self):
        _close(self)

def _publish(self, exchange, routing_key, body):
    try:
        self.channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=body,
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent
            ),
        )
    except pika.exceptions.AMQPConnectionError as e:
        raise MessageMiddlewareDisconnectedError(f"conexion failed, error: {e}")
    except pika.exceptions.AMQPError as e:
        raise MessageMiddlewareMessageError(f"error: {e}")

def _start_consuming(self, on_message_callback):
    def handle_callback(channel, method, properties, body):
        delivery_tag = method.delivery_tag

        def ack():
            channel.basic_ack(delivery_tag=delivery_tag)

        def nack():
            channel.basic_nack(delivery_tag=delivery_tag, requeue=True)

        on_message_callback(body, ack, nack)

    try:
        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=handle_callback,
            auto_ack=False,
        )
        self.consuming = True
        self.channel.start_consuming()
    except pika.exceptions.AMQPConnectionError as e:
        raise MessageMiddlewareDisconnectedError(f"conexion failed, error: {e}")
    except pika.exceptions.AMQPError as e:
        raise MessageMiddlewareMessageError(f"error: {e}")
    finally:
        self.consuming = False

def _stop_consuming(middleware):
    try:
        if not middleware.consuming:
            return
        middleware.channel.stop_consuming()
        middleware.consuming = False
    except pika.exceptions.AMQPConnectionError as e:
        raise MessageMiddlewareDisconnectedError(f"conexion failed, error: {e}")

def _close(middleware):
    try:
        if middleware.connection.is_open:
            if middleware.consuming:
                middleware.channel.stop_consuming()
                middleware.consuming = False
            if middleware.channel.is_open:
                middleware.channel.close()
            middleware.connection.close()
    except pika.exceptions.AMQPError as e:
        raise MessageMiddlewareCloseError(f"error: {e}")