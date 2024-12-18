import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

while True:
    try:
        # Прочитаем файл csv
        df = pd.read_csv('./logs/metric_log.csv')

        # Строим гистограмму
        sns_plot = sns.histplot(df.absolute_error, kde=True, color="orange")

        plt.savefig('./logs/error_distribution.png')
        # Закроем поток вывода в файл, что бы графики не накладывались
        plt.close()

        print('Файл успешно сохранен')
    except Exception as e:
        print(f"Произошла ошибка: {e}")
