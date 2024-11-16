# Импорт необходимых библиотек
from flask import Flask, request, jsonify
import pandas as pd
import numpy as np
import os

# Инициализация Flask
app = Flask(__name__)

# Пути к файлам
folder_path = r"C:\Users\Umaro\OneDrive\Рабочий стол\viborka"
csv_names = os.listdir(folder_path)

"""Данная функция обрабатывает данные из CSV-файла, выделяет определенные интервалы,
   выполняет вычисления и анализ нарушения устойчивости,
   а затем возвращает результаты в виде словаря."""
def pre_processing(csv_name, window_size):
    #Размер входного окна
    windows_size_ref = int(window_size / (10))

    #Величина на которую отступаем влево, относительно окончания возмущения
    left_border = windows_size_ref + 15

    #Величина на которую отступаем вправо, относительно окончания возмущения
    right_border = windows_size_ref + 110

    #Ведем подсчет сколько по итогу возмущений использовалось для обучения и тестирования
    contingency = 0

    #Итоговый датафрейм
    result = pd.DataFrame()

    #Определяем было ли нарушение устойчивости
    is_stability_fall = 0

    #Определяем время нарушения устойчивости
    stability_fall_time = None

    #Возвращаемый словарь
    dict_result =  {
                    'res': result,
                    'is_fall': is_stability_fall,
                    'time': stability_fall_time,
                    'cont': contingency,
                    }
    #Список с точками плюс минус ноль
    trans_list = []

    # Преобразуем полученные данные в датафрейм
    new_colon_name = ['t', 'delta_BoGES',
                      'delta_Chita', 'delta_Belovo',
                      'delta_Bereza', 'delta_Bratsk',
                      'delta_Gus', 'delta_Irkutsk',
                      'delta_Krai', 'delta_NVartovsk', 'p']
    transient_init = pd.read_csv(f'{csv_name}',
                            delimiter=";",
                            decimal=",",
                            index_col=False,
                            on_bad_lines='warn',
                            encoding='cp866')
    transient_init.columns = new_colon_name

    #Ищем индекс, от которого будем вести отсчет, он обозначает окончание возмущения
    for index, row in transient_init.iterrows():
        if index == 0:
            continue
        if transient_init['t'].loc[index] - transient_init['t'].loc[index - 1] < 0.001:
            index_start = index
            trans_list.append(index_start)
    # Считаем необходимые данные
    transient = transient_init[index_start - left_border : index_start + right_border:]
    transient_56 = transient_init[index_start - left_border - 3 :index_start + right_border:]
    # Избавляемся от плюс минус нуля
    if index_start in transient.index:
      transient = transient.drop(index_start)
    if index_start in transient_56.index:
      transient_56 = transient_56.drop(index_start)
    # Задаем новые индексы
    transient = transient.reset_index(drop=True)
    transient_56 = transient_56.reset_index(drop=True)

    # Выполним расчеты относительно генератора Богучанской ГЭС
    transient.loc[:, 'delta'] = transient['delta_BoGES'] - transient['delta_Bereza']
    transient_56.loc[:, 'delta'] = transient_56['delta_BoGES'] - transient_56['delta_Bereza']

    if transient['delta'].iloc[0] < 0:
        # Поднимаем значения вверх по оси x до нуля
        transient['delta'] = transient['delta'] - (transient['delta'].iloc[0])

    # Переводим данные в в формат numpy
    time_1 = transient_56['t'].to_numpy()
    delta_1 = transient_56['delta'].to_numpy()
    # Считаем производную
    w = np.diff(delta_1) / (np.diff(time_1) * 1000)
    # Массив для расчета ускорения
    time_2 = np.delete(time_1, 0)
    # Костыль для избавления от выбросов
    w_2 = pd.Series(w)
    w_2 = w_2.replace(0, None)
    w_2 = w_2.ffill()
    w_2 = w_2.to_numpy()
    # Считаем ускорение
    a = np.diff(w_2) / (np.diff(time_2) * 1000)
    # Удаляем первую строчку, для соблюдения размерности
    w_1 = np.delete(w, 0)
    w_1 = np.delete(w_1, 0)
    a = np.delete(a, 0)
    transient["w"] = w_1
    transient["a"] = a
    transient["w"] = transient["w"].replace(0, None)
    transient["w"] = transient["w"].ffill()
    # Определяем время нарушения устойчивости и факт нарушения устойчивости
    for index, row in transient.iterrows():
        if row['delta'] > 180:
            is_stability_fall = 1
            stability_fall_time = (row['t'] - transient_init['t'].loc[index_start]) * 1000
            break
    # Возращаем единичку, т.к. у нас это возмущение идет в обущающую выборку
    contingency += 1
    # Получаем итоговый датафрейм
    result = pd.concat([result, transient], ignore_index=True)
    dict_result = {
        'res': result,
        'is_fall': is_stability_fall,
        'time': stability_fall_time,
        'cont': contingency
    }
    return dict_result

"""Данная функция позволяет провести обработку данных из нескольких файлов
   и собрать результаты вместе для дальнейшего анализа и использования."""
def get_df_tran(csv_names, window_size):
    result = pd.DataFrame()
    time_fall_list = []
    is_fall_list = []
    contingency_1 = []

    for i in range(0, len(csv_names)):
        dict_result_1 = pre_processing(csv_name=csv_names[i], window_size=window_size)
        result = pd.concat([result,
                            dict_result_1['res']],
                            ignore_index=True)
        time_fall_list.append(dict_result_1['time'])
        is_fall_list.append(dict_result_1['is_fall'])
        contingency_1.append(dict_result_1['cont'])
        i += 1
    total_result = {
                    'data': result,
                    'time_lst': time_fall_list,
                    'is_fall': is_fall_list,
                    'cont1': contingency_1,
                    }

    time = pd.DataFrame(total_result['time_lst'])
    print(total_result['data'].info())
    print(f"Количество случаев превышения угла 180 градусов: {sum(total_result['is_fall'])}")
    print(f"Общее количество рассмотренных возмущений {sum(total_result['cont1'])}")
    print(f"Отношение процент случаев нарушения устойчивости { sum(total_result['is_fall']) / sum(total_result['cont1']) * 100 }")
    print(time.describe())
    return total_result

"""Данная функция загружает данные из CSV-файла, находит точки окончания возмущений,
   выбирает данные после этих точек, добавляет информацию о разности delta_1 и delta_2
   и возвращает итоговый датасет с этой информацией."""
def data_graph(csv_name, window_size):
    # Данные, которые нам нужны для исследования
    # Размер входного окна
    windows_size_ref = int(window_size / (10))
    # Величина на которую отступаем влево, относительно окончания возмущения
    right_border = 60
    # Итоговый датафрейм
    result = pd.DataFrame()
    # Список с точками плюс минус ноль
    trans_list = []
    # Преобразуем полученные данные в датафрейм
    new_colon_name = ['t', 'delta_BoGES',
                      'delta_Chita', 'delta_Belovo',
                      'delta_Bereza', 'delta_Bratsk',
                      'delta_Gus', 'delta_Irkutsk',
                      'delta_Krai', 'delta_NVartovsk', 'p']
    transient_init = pd.read_csv(f'{csv_name}',
                                 delimiter=";",
                                 decimal=",",
                                 index_col=False,
                                 on_bad_lines='warn',
                                 encoding='cp866')
    transient_init.columns = new_colon_name
    #transient_init['t'] = pd.to_numeric(transient_init['t'], errors='coerce')
    #transient_init['delta_1'] = pd.to_numeric(transient_init['delta_1'], errors='coerce')
    #transient_init['delta_2'] = pd.to_numeric(transient_init['delta_2'], errors='coerce')

    # Ищем индекс, который от которого будем вести отсчет, обозначет окончание возмущения
    for index, row in transient_init.iterrows():
        if index == 0:
            continue
        if transient_init['t'].loc[index] - transient_init['t'].loc[index - 1] < 0.001:
            index_start = index
            trans_list.append(index_start)
            # Считаем необходимые данные
    transient = transient_init[index_start: index_start + right_border:]
    # Задаем новые индексы
    transient = transient.reset_index(drop=True)

    # Выполним расчеты относительно Гусиноозерской ГРЭС
    transient.loc[:, 'delta'] = transient['delta_BoGES'] - transient['delta_Gus']
    result = pd.concat([result, transient], ignore_index=True)
    return result

"""Эта функция предназначена для получения итогового датафрейма,
   содержащего данные после обработки функцией data_graph для всех CSV-файлов."""

def get_graph(csv_names, window_size):
    result = pd.DataFrame()
    for i in range(0, len(csv_names)):
        result = pd.concat([result, data_graph(csv_name=csv_names[i], window_size=window_size)], ignore_index=True)
    return result

# Функция для преобразования данных из CSV-файла
def sets_create(csv_file):

    # Обработка CSV-файла
    result = get_df_tran(csv_file, window_size=100)['data']

    # Выделяем необходимые признаки
    result = result[['t', 'delta', 'w', 'a', 'p']]

    # Извлекаем метку времени и удалим из начального датафрейма
    date_time = pd.to_datetime(result.pop('t'), unit='ms')
    timestamp_s = date_time.map(pd.Timestamp.timestamp)

    # Разобьем полученный временной ряд на 3 выборки, тренировочную, валидационную, тестовую
    n = len(result)

    train_df = result[0: 274 * 144]
    train_df = train_df.fillna(0)

    val_df = result[274 * 144: 313 * 144]
    val_df = val_df.fillna(0)

    test_df = result[313 * 144:]
    test_df = test_df.fillna(0)

    print(n)

    # Нормализация данных
    train_mean = train_df.mean()
    train_std = train_df.std()

    train_df = (train_df - train_mean) / train_std
    val_df = (val_df - train_mean) / train_std
    test_df = (test_df - train_mean) / train_std

    return train_df, val_df, test_df



