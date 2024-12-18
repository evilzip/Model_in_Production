import pika
import numpy as np
import json
from sklearn.datasets import load_diabetes
import time
from datetime import datetime

# Задержка времени
TIME_SLEEP = 5

# Создаём бесконечный цикл для отправки сообщений в очередь
while True:
    try:
        # Загружаем датасет о диабете
        X, y = load_diabetes(return_X_y=True)
        # Формируем случайный индекс строки
        random_row = np.random.randint(0, X.shape[0] - 1)

        # Создаём подключение по адресу rabbitmq:
        connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
        channel = connection.channel()

        # Создаём очередь y_true
        channel.queue_declare(queue='y_true')
        # Создаём очередь features
        channel.queue_declare(queue='features')

        # Зададим mesage_id
        message_id = datetime.timestamp(datetime.now())

        # Публикуем сообщение в очередь y_true
        message_y_true = {
            'id': message_id,
            'body': y[random_row]
        }
        channel.basic_publish(exchange='',
                              routing_key='y_true',
                              body=json.dumps(message_y_true))
        print('Сообщение с правильным ответом отправлено в очередь y_true')

        # Публикуем сообщение в очередь features
        message_features = {
            'id': message_id,
            'body': list(X[random_row])
        }
        channel.basic_publish(exchange='',
                              routing_key='features',
                              body=json.dumps(message_features))
        print('Сообщение с вектором признаков отправлено в очередь features')

        # Закрываем подключение
        connection.close()
        # Задержка
        time.sleep(TIME_SLEEP)
    except Exception as e:
        print(f"Произошла ошибка: {e}")
