import pika
import json
from datetime import datetime
import csv
import pandas as pd

try:
    # Создаём подключение к серверу на локальном хосте
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()
    # Объявляем очередь y_true
    channel.queue_declare(queue='y_true')
    # Объявляем очередь y_pred
    channel.queue_declare(queue='y_pred')

    # Создаем пустой датафрэйм для хранения сообщений
    col_names = ['id', 'y_true', 'y_pred', 'absolute_error']
    metric_df = pd.DataFrame(columns=col_names)
    metric_df.to_csv('./logs/metric_log.csv')


    # Создаём функцию callback для обработки данных из очереди
    def callback(ch, method, properties, body):
        print(f'Из очереди {method.routing_key} получено значение {json.loads(body)}')
        message = json.loads(body)
        answer_string = f"Из очереди {method.routing_key} получено значение {message['id']}:{message['body']}"
        # Запись в TXT file. Для тестировки
        # print('method.routing_key', method.routing_key)
        # with open('./logs/labels_log.txt', 'a') as log:
        #     log.write(answer_string + '\n')
        if method.routing_key == 'y_true':  # y_true пишем в dataframe
            id = message['id']
            y_true = message['body']
            metric_df.at[len(metric_df), 'id'] = id
            metric_df.at[len(metric_df) - 1, 'y_true'] = y_true

        elif method.routing_key == 'y_pred':  # y_pred пишем в dataframe
            id = message['id']
            y_pred = message['body']
            metric_df.at[len(metric_df) - 1, 'y_pred'] = y_pred

        if len(metric_df) > 0:  # сохраняем dataframe to CSV
            a = metric_df.at[len(metric_df) - 1, 'y_true']
            b = metric_df.at[len(metric_df) - 1, 'y_pred']
            metric_df.at[len(metric_df) - 1, 'absolute_error'] = abs(a - b)
            metric_df.to_csv('./logs/metric_log.csv')


    # Извлекаем сообщение из очереди y_true
    channel.basic_consume(
        queue='y_true',
        on_message_callback=callback,
        auto_ack=True
    )
    # Извлекаем сообщение из очереди y_pred
    channel.basic_consume(
        queue='y_pred',
        on_message_callback=callback,
        auto_ack=True
    )

    # Запускаем режим ожидания прихода сообщений
    print('...Ожидание сообщений, для выхода нажмите CTRL+C')
    channel.start_consuming()
except Exception as e:
    print(f"Произошла ошибка: {e}")
