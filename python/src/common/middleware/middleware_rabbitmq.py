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

            
    def stop_consuming(self):
        try:
            if not self.consuming:
                return
            self.channel.stop_consuming()
            self.consuming = False
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"conexion failed, error: {e}")

    def send(self, message):
        try:    
            self.channel.basic_publish(
                exchange='',
                routing_key=self.queue_name,
                body=message,
                properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent)
            )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"conexion failed, error: {e}") 
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(f"error: {e}")

    def close(self):
        try: 
            if self.connection.is_open:
                if self.consuming:
                    self.channel.stop_consuming()
                    self.consuming = False
                if self.channel.is_open:
                    self.channel.close()
                self.connection.close()
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareCloseError(f"error': {e}")


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):

    def __init__(self, host, exchange_name, routing_keys):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=exchange_name, exchange_type='direct')
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.queue_name = result.method.queue
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        for key in routing_keys:
            self.channel.queue_bind(exchange=exchange_name,
                               queue=self.queue_name,
                               routing_key=key)
        self.consuming = False

    def start_consuming(self, on_message_callback):
            def handle_callback(channel, method, properties, body):
                def ack():
                    channel.basic_ack(delivery_tag=method.delivery_tag)

                def nack():
                    channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

                on_message_callback(body, ack, nack)

            try:
                self.channel.basic_qos()
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


    def stop_consuming(self):
        try:
            if not self.consuming:
                return
            self.channel.stop_consuming()
            self.consuming = False
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"conexion failed, error: {e}")

    def send(self, message):
        try:
            for key in self.routing_keys:
                self.channel.basic_publish(
                    exchange=self.exchange_name,
                    routing_key=key,
                    body=message,
                    properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent)
            )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"conexion failed, error: {e}")
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(f"error: {e}")


    def close(self):
        try:
            if self.connection.is_open:
                if self.consuming:
                    self.channel.stop_consuming()
                    self.consuming = False
                if self.channel.is_open:
                    self.channel.close()
                self.connection.close()
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareCloseError(f"error': {e}")

