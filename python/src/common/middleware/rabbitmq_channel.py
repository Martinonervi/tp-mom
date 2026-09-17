import pika
from .middleware import MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError, MessageMiddlewareCloseError


class RabbitMQChannel:

    def __init__(self, host, prefetch_count=1):
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host))
            self.channel = self.connection.channel()
            self.channel.basic_qos(prefetch_count=prefetch_count)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"conexion failed, host: {host}, error: {e}")
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(f"error: {e}")
        self.consuming = False

    def setup_queue(self, name):
        try:
            result = self.channel.queue_declare(queue=name, durable=True)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"conexion failed, error: {e}")
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(f"error: {e}")
        return result.method.queue

    def subscribe_to_exchange(self, name, routing_keys, exchange_type='direct'):
        try:
            self.channel.exchange_declare(exchange=name, exchange_type=exchange_type)
            result = self.channel.queue_declare(queue='', exclusive=True)
            queue_name = result.method.queue
            for key in routing_keys:
                self.channel.queue_bind(exchange=name, queue=queue_name, routing_key=key)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"conexion failed, error: {e}")
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(f"error: {e}")
        return queue_name

    def publish(self, exchange, routing_key, body):
        try:
            self.channel.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                body=body,
                properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent)
            )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"conexion failed, error: {e}")
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(f"error: {e}")

    def consume(self, queue, on_message_callback):
        def handle_callback(channel, method, properties, body):
            delivery_tag = method.delivery_tag

            def ack():
                channel.basic_ack(delivery_tag=delivery_tag)

            def nack():
                channel.basic_nack(delivery_tag=delivery_tag, requeue=True)

            on_message_callback(body, ack, nack)

        try:
            self.channel.basic_consume(
                queue=queue,
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
            raise MessageMiddlewareCloseError(f"error: {e}")

