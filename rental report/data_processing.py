from pathlib import Path
import pandas as pd
import numpy as np
import datetime
from dateutil.relativedelta import relativedelta
import os
import logging
from typing import List, Tuple
import cx_Oracle

from sap import sap_main_zco
from sap import sap_wagon_numbers_iw38
from sap import sap_RPS_zpm
from sap import sap_contract_zpm
from sap import sap_RM_iw38
from sap import sap_spravka_RM_SQ01

from utils import (
    cutting,
    svod_vtbl_any,
    svod_vtbl_opr,
    for_concat_any,
    for_concat_opr,
    itog_for_vtbl,
    list_vtbl,
    svod_any,
    svod_oprihod,
    get_ownership_df
)

# КОНСТАНТНЫЕ ПЕРЕМЕННЫЕ
logger = logging.getLogger("my_log_rent")
logger.info("from data_processing.py")

# month_start = input('Первый запрашиваемый месяц: ') # УКАЗАТЬ ИНТЕРЕСУЮЩИЕ МЕСЯЦА
# month_finish = input('Последний запрашиваемый месяц: ')
# year = input('Запрашиваемый год: ')

""" будем его использовать для определения месяца и года"""
# report_date = datetime.datetime.now()

""" объявляем папку и создаем если ее нет"""
# project_path  = Path(os.getcwd(), "folder_with_uploads_for_rent")
# project_path = Path(r"C:/Users/ChudinovskikhAO/Desktop/arenda/project") # задаем путь к папке с проектом

# #spravka = Path(os.getcwd(), 'spravka.xlsx')
# spravka = Path(r"C:/Users/ChudinovskikhAO/Desktop/arenda/project", 'spravka.xlsx')

# if not project_path.exists():
#     logger.debug("Создаем папку для выгрузок отчета аренды")
#     project_path.mkdir()
#     logger.debug(f"Путь для выгрузок отчета аренды {project_path.absolute()}")

# начать основную функцию (project_path, report_date, spravka, button_1)

def rent_main(project_path, report_date, spravka, button_RM):
    '''
    Функция формирует основной массив с данными.
    1. Выполняется выгрузка из ZCO_LINE_ITEMS
    2. По ТОРО-заказам из IW38 подтягиваются номера вагонов, если по ним имеются пропуски
    3. По номерам вагонов из zpm_pgk_erv подтягиваются РПС, если по ним имеются пропуски
    4. По номерам вагонов из zpm_pgk_erv подтягиваются номера договоров, если по ним имеются пропуски
    5. Формируются дополнительные столбцы: Год, Месяц, Сцепка, Собственник и, если необходимо, Рабочее место
    5.1. Столбец собственник заполняется с помощью SQL-запроса
    5.2. Столбец Рабочее место заполняется с помощью 2-х выгрузок: из IW38 и SQ01

    '''
    full_data_file = Path(project_path.parent, 'full_data.xlsx')
    if not full_data_file.exists() or False: # проверяем файл в наличие или нет
        logger.debug("----Сборка основного файла----")
        month_start = (report_date.replace(day=1) + relativedelta(months=-1)).strftime("%m")
        month_finish = (report_date.replace(day=1) + relativedelta(months=-1)).strftime("%m")
        year = (report_date.replace(day=1) + relativedelta(months=-1)).strftime("%Y")

        accounts = [3661200001, 3101001000, 3661100001, 3661300001, 3502001000, 3101002000, 3502001100, 3502002000, 3501009000, 3501024000, 9110302100, 9110303100]
        # 
        df_accounts = pd.DataFrame(accounts, columns=['accounts'])

        sap_main_zco_2 = Path(project_path, 'zco.xlsx')
        if not sap_main_zco_2.exists() or False: # проверяем файл в наличии или нет
            logger.debug("Выгружаем ZCO из ZCO_LINE_ITEMS")
            sap_main_zco(df_accounts, year, month_start, month_finish, sap_main_zco_2, "ZCO_LINE_ITEMS") # если нет скачиваем
            logger.debug("ZCO выгружен. Название файла zco")
        else:
            logger.debug("Проверка наличия файла ZCO - ОК")

        logger.debug("Загружаем данные ZCO")
        df_zco = pd.read_excel(sap_main_zco_2)

        df_zco_without_wagons = df_zco[df_zco['Номер вагона'].isnull()]
        toro_order = df_zco_without_wagons['Заказ'] # получаем торо-заказов, где нет информации по вагону
        
        logger.debug("Обнаружены пропуски номеров вагонов в ZCO, требуется выгрузить IW38")
        sap_wagon_numbers_iw38_2 = Path(project_path, 'wagon_gap.xlsx')
        if not sap_wagon_numbers_iw38_2.exists() or False: # проверяем файл в наличие или нет
            logger.debug("Выгружаем пропущенные номера вагонов из IW38")
            sap_wagon_numbers_iw38(toro_order, sap_wagon_numbers_iw38_2, "IW38") # если нет скачиваем
            logger.debug("IW38 выгружен. Название файла wagon_gap")
        else:
            logger.debug("Проверка наличия IW38 файла wagon_gap - ОК")

        wagons_gap = pd.read_excel(sap_wagon_numbers_iw38_2, usecols=['Заказ', 'Название технического объекта'])

        wagons_without_RPS_1 = wagons_gap['Название технического объекта'] # список вновь подтянутых номеров вагонов, будем использовать его для подтяжки рпс
        wagons_without_RPS_1 = wagons_without_RPS_1.dropna()
        wagons_without_RPS_1 = wagons_without_RPS_1.astype('int')
        df_zco_without_RPS = df_zco[(df_zco['Род ПС'].isnull()) & (df_zco['Номер вагона'].notnull())] # вторая часть списка для подтяжки рпс
        wagons_without_RPS_2 = df_zco_without_RPS['Номер вагона']
        wagons_without_RPS = pd.concat([wagons_without_RPS_1, wagons_without_RPS_2]) # объединяем 2 списка

        logger.debug("Обнаружены пропуски РПС. Требуется выгрузка из zpm-pgk-erv")
        # а как ты выгружаешь РПС если не все вагоны известны по номеру заказа?

        sap_RPS_zpm_2 = Path(project_path, 'rps_gap.xlsx')
        if not sap_RPS_zpm_2.exists() or False: # проверяем файл в наличие или нет
            logger.debug("Выгружаем пропущенные РПС из zpm_pgk_erv")
            sap_RPS_zpm(wagons_without_RPS, sap_RPS_zpm_2, "zpm_pgk_erv") # если нет скачиваем
            logger.debug("РПС выгружены. Название файла rps_gap")
        else:
            logger.debug("Проверка наличия файла rps_gap - ОК")

        logger.debug("Добавляем номера вагонов и РПС")
        rps_gap = pd.read_excel(sap_RPS_zpm_2) # выгрузка с кодом РПС
        spravka_rps = pd.read_excel(spravka, 'Справка РПС 1') # справка соответствия кода и названия РПС
        logger.debug(f"Количество строк в rps_gap до ВПР кода с названием РПС: {rps_gap.shape[0]}")
        rps_gap = pd.merge(rps_gap, spravka_rps[['Род ПС код', 'Род ПС']], how='left', left_on='Подрод вагона УУ/Cognos', right_on='Род ПС')
        rps_gap = rps_gap.drop_duplicates()
        logger.debug(f"Количество строк в rps_gap после ВПР кода с названием РПС: {rps_gap.shape[0]}")
        logger.debug(f"Количество строк в zco до ВПР номеров вагонов: {df_zco.shape[0]}")
        df_zco = pd.merge(df_zco, wagons_gap[['Заказ','Название технического объекта']], how='left', on='Заказ') # впр по заказу торо пропущенных номеров вагонов
        logger.debug(f"Количество строк в zco после ВПР номеров вагонов: {df_zco.shape[0]}")
        df_zco.loc[df_zco['Номер вагона'].isnull(), 'Номер вагона'] = df_zco['Название технического объекта'] # вставляем из присоединенной колонки в основную
        df_zco = pd.merge(df_zco, rps_gap[['Подрод вагона УУ/Cognos', '№ вагона', 'Род ПС код']], how='left', left_on='Номер вагона', right_on='№ вагона') # впр по номеру вагона рода ПС
        logger.debug(f"Количество строк в zco после ВПР РПС: {df_zco.shape[0]}")
        df_zco.loc[df_zco['Род ПС'].isnull(), 'Род ПС'] = df_zco['Род ПС код'] # вставляем из присоединенного столбца код РПС
        df_zco.loc[df_zco['Род ПС.1'].isnull(), 'Род ПС.1'] = df_zco['Подрод вагона УУ/Cognos'] # вставляем из присоединенного столбца название РПС
        df_zco.drop(['Название технического объекта', 'Подрод вагона УУ/Cognos', '№ вагона', 'Род ПС код'], axis=1, inplace=True) # удаляем лишние колонки
        df_zco_without_contract = df_zco[(df_zco['№ договора'].isnull()) & (df_zco['Номер вагона'].notnull())] 
        wagons_without_contract = df_zco_without_contract['Номер вагона']
        wagons_without_contract = pd.DataFrame(wagons_without_contract.unique())
        logger.debug("Номера вагонов и РПС добавлены")

        logger.debug("Добавляем номера договоров")
        sap_contract_zpm_2 = Path(project_path, 'dogovor_gap.xlsx')
        if not sap_contract_zpm_2.exists() or False: # проверяем файл в наличие или нет
            logger.debug("Выгружаем пропущенные номера договоров из zpm_pgk_erv")
            sap_contract_zpm(wagons_without_contract, sap_contract_zpm_2, "zpm_pgk_erv") # если нет скачиваем
            logger.debug("Номера договоров выгружены. Название файла contract_gap")
        else:
            logger.debug("Проверка наличия файла contract_gap - ОК")

        contract_gap = pd.read_excel(sap_contract_zpm_2)

        # впр номеров договоров
        df_zco = pd.merge(df_zco, contract_gap[['№ вагона', 'Взятие в аренду: Номер приложения RCM']], how='left', left_on='Номер вагона', right_on='№ вагона') # впр по номеру вагона номера договора
        logger.debug(f"Количество строк в zco после ВПР номера договора: {df_zco.shape[0]}")
        df_zco.loc[df_zco['№ договора'].isnull(), '№ договора'] = df_zco['Взятие в аренду: Номер приложения RCM'] # вставляем из присоединенного столбца номер договора
        df_zco.drop(['№ вагона', 'Взятие в аренду: Номер приложения RCM'], axis=1, inplace=True) # удаляем лишние колонки

        df_zco = df_zco[(df_zco['Вид работ ТОРО']==111) |
                        (df_zco['Вид работ ТОРО']==112) |
                        (df_zco['Вид работ ТОРО']==113) |
                        (df_zco['Вид работ ТОРО']==114) |
                        (df_zco['Вид работ ТОРО']==121) |
                        (df_zco['Вид работ ТОРО']==122) |
                        (df_zco['Вид работ ТОРО']==190)] # выбираем из выгрузки интересующие виды работ
        logger.debug(f"Количество строк в zco после отбора только необходимых видов работ: {df_zco.shape[0]}")
        df_zco[['Год1', "Месяц", "Сцепка"]] = 0 # СОЗДАЕМ ПУСТЫЕ СТОЛБЦЫ - наверно можно удалить эту строчку
        df_zco["Год1"] = df_zco['Д/докум.'].dt.year.astype(int) # ЗАПОЛНЯЕМ ГОД
        df_zco["Месяц"] = df_zco['Д/докум.'].dt.month.astype(int) # ЗАПОЛНЯЕМ МЕСЯЦ
        df_zco["Номер вагона"] = df_zco['Номер вагона'].astype(int) # ЗАПОЛНЯЕМ СЦЕПКУ
        df_zco["Сцепка"] = df_zco['Номер вагона'].astype(str)+df_zco['Месяц'].astype(str)+df_zco['Год1'].astype(str)
        df_zco['№ договора'] = df_zco['№ договора'].fillna(0) # необходимо для корректного создания сводной, мб лучше заменять не на 0, но на "НД" нельзя - возникает ошибка merge str и int

        # заглушка на собственника: подгружаем из других файлов ЗАМЕНИТЬ НА КОМАНДОР
        # xls = pd.ExcelFile('064F3F20.xlsx') 
        # oprihod_owners = pd.read_excel(xls, 'Оприход', usecols='BI:BJ') TODO поставить названия столбцов
        # plr_owners = pd.read_excel(xls, 'ПЛР', usecols='BQ:BR')
        # tr2_owners = pd.read_excel(xls, 'ТР2', usecols='BM:BN')
        # ppv_owners = pd.read_excel(xls, 'ППВ', usecols='BI:BJ')

        # plr_owners.rename(columns = {'собственник УА' : 'Собственник УА'}, inplace = True)
        # tr2_owners.rename(columns = {'Сцепить' : 'сцеп', 'собственник УА' : 'Собственник УА'}, inplace = True) # приводим заголовки к единому формату
        # owners = pd.concat([oprihod_owners, plr_owners, tr2_owners, ppv_owners], ignore_index=True) # объединяем
        # owners = owners.drop_duplicates(subset='сцеп') # удаляем дупликаты
        # owners['сцеп'] = owners['сцеп'].astype(str)

        # df_zco = pd.merge(df_zco, owners[['сцеп','Собственник УА']], how='left', left_on='Сцепка', right_on='сцеп') # ВПР СОБСТВЕННИКА
        # logger.debug(f"Количество строк в zco после ВПР собственника: {df_zco.shape[0]}")
        # df_zco.drop('сцеп', axis=1, inplace=True) # УДАЛЯЕМ ЛИШНИЕ КОЛОНКИ

        # КОНЕЦ ЗАГЛУШКИ НА СОБСТВЕННИКА
         
        # for_test = df_zco[["Номер вагона","Д/докум."]].drop_duplicates()
        # with pd.ExcelWriter('test_omners_2607.xlsx', engine='xlsxwriter') as writer:  
        #     for_test.to_excel(writer, index=False)
        # #принт нужен?
        # print(for_test.isna().sum())

        logger.debug("Подтягивается собственник")
        wagnum_lst = df_zco[["Номер вагона","Д/докум."]].drop_duplicates().to_dict(orient="split")["data"]
        logger.debug(f"Количество вагонов для определения собственника: {len(wagnum_lst)}")

        len_chunk = 2000 # задаем по сколько вагонов отправлять в один запрос к командору
        cnt_chunks = len(wagnum_lst) // len_chunk # целочисленным делением определеям, сколько групп вагонов у нас есть
        wagnum_chunks = [wagnum_lst[i:i + len_chunk] for i in range(0, len(wagnum_lst), len_chunk)] # выбираем строчки из wagnum_lst по len_chunk штук

        owner_list = []
        for i, chunk in enumerate(wagnum_chunks):
            logger.debug(f"Выгружено вагонов: [{i + 1}/{cnt_chunks + 1}]")
            owner_df = get_ownership_df(chunk)
            owner_list.append(owner_df)
            logger.debug(f"Всего выгружана вагонов [{i + 1}/{cnt_chunks + 1}]") # отпраляем группы в командор
        owner_df = pd.concat(owner_list)

        owner_df.drop_duplicates(subset=['wagnum', 'report_dt'], keep='first', inplace=True)

        # owner_df['year'] = owner_df['report_dt'].astype('str').str.extract(r'(\d{4})-\d{2}-\d{2}') # формируем сцепку в дф с собственниками
        # owner_df['mon'] = owner_df['report_dt'].astype('str').str.extract(r'\d{4}-(\d{2})-\d{2}').astype('int') 
        # owner_df['сцеп'] = owner_df['wagnum'].astype('str')+owner_df['mon'].astype('str')+owner_df['year']

        df_zco = pd.merge(df_zco, owner_df[['wagnum' ,'owner', 'report_dt']], how='left', left_on=['Номер вагона','Д/докум.'], right_on=['wagnum', 'report_dt'])
        logger.debug(f"Количество строк в zco после ВПР собственника: {df_zco.shape[0]}")
        df_zco.drop(['wagnum', 'report_dt'], axis=1, inplace=True) # УДАЛЯЕМ ЛИШНИЕ КОЛОНКИ
        df_zco.rename(columns = {'owner' : 'Собственник УА'}, inplace = True)
        with pd.ExcelWriter('owners.xlsx', engine='xlsxwriter') as writer:  
            owner_df.to_excel(writer, index=False)

        # ПЕРЕИМЕНОВАНИЕ НЛМК В ПГК
        df_zco.loc[(df_zco['Собственник УА'] == 'ПАО "НЛМК"'), 'Собственник УА'] = 'ОАО ПГК' # корректно ли?
        df_zco.loc[(df_zco['Собственник УА'] == 'ПАО "ПГК"'), 'Собственник УА'] = 'ОАО ПГК'

        df_zco_cut = df_zco[(df_zco['Собственник УА'] != 0) & (df_zco['Собственник УА'] != 'ОАО ПГК') & (df_zco['Собственник УА'].notnull())] # УБИРАЕМ ПГК ИЗ СОБСТВЕННИКОВ

        if button_RM == 1: 
        # НАЧАЛО ВЫГРУЗКИ РАБОЧИХ МЕСТ

            toro_orders_for_RM = pd.DataFrame(df_zco_cut['Заказ'].unique()) # ФОРМИРУЕМ СПИСОК ТОРО-ЗАКАЗОВ ДЛЯ ВЫГРУЗКИ РАБОЧИХ МЕСТ

            sap_RM_iw38_2 = Path(project_path, 'RM.xlsx')
            if not sap_RM_iw38_2.exists() or False: # проверяем файл в наличие или нет
                logger.debug("Выгружаем рабочие места из IW38")
                sap_RM_iw38(toro_orders_for_RM, sap_RM_iw38_2, 'IW38') # если нет скачиваем
                logger.debug("Рабочие места выгружена. Название файла RM")
            else:
                logger.debug("Проверка наличия файла RM - ОК")

            sap_spravka_RM_SQ01_2 = Path(project_path, 'spravka_RM.xlsx')
            if not sap_spravka_RM_SQ01_2.exists() or False: # проверяем файл в наличие или нет
                logger.debug("Выгружаем справочник рабочих мест из SQ01")
                sap_spravka_RM_SQ01(sap_spravka_RM_SQ01_2, 'SQ01') # если нет скачиваем
                logger.debug("Справочник рабочих мест выгружен. Название файлы spravka_RM")
            else:
                logger.debug("Проверка наличия файла spravka_RM - ОК")

            RM = pd.read_excel(sap_RM_iw38_2) # выгрузка рабочих мест
            spravka_RM = pd.read_excel(sap_spravka_RM_SQ01_2) # выгрузка справки соответствия кода и названия рабочего места
            spravka_RM = spravka_RM.drop_duplicates(subset='РабМесто')
            logger.debug(f"Количество строк в листе РМ до ВПР со справочником: {RM.shape[0]}")
            RM = pd.merge(RM, spravka_RM[['РабМесто','Краткое название']], how='left', left_on='Управл. рабоч. место', right_on='РабМесто') # ВПР НАЗВАНИЯ РАБОЧЕГО МЕСТА
            logger.debug(f"Количество строк в листе РМ после ВПР со справочником: {RM.shape[0]}")
            RM.rename(columns = {'Заказ' : 'Заказ_РМ'}, inplace = True)
            df_zco_cut = pd.merge(df_zco_cut, RM[['Заказ_РМ','Краткое название']], how='left', left_on='Заказ', right_on='Заказ_РМ') # ВПР РАБОЧЕГО МЕСТА В ППВ
            df_zco_cut.drop('Заказ_РМ', axis=1, inplace=True) # УДАЛЯЕМ ЛИШНИЕ КОЛОНКИ
            df_zco = pd.merge(df_zco, RM[['Заказ_РМ','Краткое название']], how='left', left_on='Заказ', right_on='Заказ_РМ') # ВПР РАБОЧЕГО МЕСТА В ППВ
            logger.debug(f"Количество строк в zco после ВПР рабочих мест: {df_zco.shape[0]}")
            df_zco.drop('Заказ_РМ', axis=1, inplace=True) # УДАЛЯЕМ ЛИШНИЕ КОЛОНКИ
            # КОНЕЦ ВЫГРУЗКИ РАБОЧИХ МЕСТ

    else:
        logger.debug("Проверка наличия файла full_data - ОК")
        logger.debug("Выполняется чтение full_data")
        df_zco = pd.read_excel(full_data_file, 'Data')
        df_zco.loc[(df_zco['Собственник УА'] == 'ПАО "НЛМК"'), 'Собственник УА'] = 'ОАО ПГК'
        df_zco.loc[(df_zco['Собственник УА'] == 'ПАО "ПГК"'), 'Собственник УА'] = 'ОАО ПГК'
        df_zco_cut = df_zco[(df_zco['Собственник УА'] != 0) & (df_zco['Собственник УА'] != 'ОАО ПГК') & (df_zco['Собственник УА'].notnull())] # УБИРАЕМ ПГК ИЗ СОБСТВЕННИКОВ

    return df_zco, df_zco_cut

def load_data(df_zco, project_path: Path):
    '''
    Функция выгружает в excel сформированный датафрейм, фильтруя его для формирования необходимых листов
    
    '''
    logger.debug("Добавляем листы Оприход, ТР-2, ПЛР, ППВ, Data")
    oprihod_full, plr_full, tr2_full, ppv_full = cutting(df_zco)

    with pd.ExcelWriter(Path(project_path.parent, 'full_data.xlsx'), engine='xlsxwriter') as writer:  
       
        sheets = [[oprihod_full, 'Оприход'], [tr2_full, 'ТР-2'], [plr_full, 'ПЛР'], [ppv_full, 'ППВ'], [df_zco, 'Data']]
        for i in range(5):
            sheets[i][0].to_excel(writer, sheets[i][1], index=False)
            sheet = writer.sheets[sheets[i][1]]
            sheet.autofilter(0,0,0,61) # first_row, first_col, last_row, last_col
            cell_format = writer.book.add_format() # создаем формат для первой строки
            cell_format1 = writer.book.add_format() # создаем формат для остальных строк
            cell_format1.set_font_size(8) # устанавливаем размер шрифта для всех строк
            cell_format.set_font_color('white')
            cell_format.set_bg_color('#560319')
            cell_format.set_font_size(8) # устанавливаем размер шрифта для 1 строки   
            sheet.write_row(0, 0, sheets[i][0].columns, cell_format) # записываем формат в первую строку
            for j in range(100000):
                sheet.set_row(j, 10, cell_format1) # устанавливаем высоту строки для всех строк



def otchet(df_zco_cut, spravka: Path, report_date):
    '''
    Функция формирует и выгружает в excel Отчет для управления аренды, используя данные сформированного датафрейма
    1. Из отфильтрованных массивов Оприход, ТР-2, ПЛР и ППВ формируются небходимые сводные
    2. Данные из сформированных сводных агрегируются, собираются в сводные по РПС и Собственнику. Заполняется отчетный "первый" лист Отчета
    3. Формируются сводные по вагонам, арендуемых у ВТБ-Лизинг
    4. Формируются сводные по вагонам, арендуемых у ВТБ-Лизинг и ремонтируемых в 2022 году
    5. Формируется отчетный лист по вагонам, арендуемых у ВТБ-Лизинг
    6. Сводные и Отчетные листы экспортируются в Excel
    
    '''
    logger.debug("----Формирование отчета----")

    month_start = (report_date.replace(day=1) + relativedelta(months=-1)).strftime("%m")
    year = (report_date.replace(day=1) + relativedelta(months=-1)).strftime("%Y")

    oprihod, plr, tr2, ppv = cutting(df_zco_cut)
    kontragent = pd.read_excel(spravka, 'Справка контрагенты')
    spravka_rps_2 = pd.read_excel(spravka, 'Справка РПС 2')
    logger.debug("Справочники контрагентов и РПС загружены")

    # Работа с ТР-2
    logger.debug("Формуруем сводную по ТР-2")
    tr2_svod = svod_any(tr2, spravka_rps_2, kontragent)
    logger.debug("Сводная ТР2  сформирована")

    # Работа с Оприход
    logger.debug("Формуруем сводную Оприход")
    oprihod_svod = svod_oprihod(oprihod, spravka_rps_2, kontragent)
    logger.debug("Сводная Оприход  сформирована")

    # Работа с ПЛР
    logger.debug("Формуруем сводную ПЛР")
    plr_svod = svod_any(plr, spravka_rps_2, kontragent)
    logger.debug("Сводная ПЛР  сформирована")

    # Работа с ППВ
    logger.debug("Формуруем сводную ППВ")
    ppv_svod = svod_any(ppv, spravka_rps_2, kontragent)
    logger.debug("Сводная ППВ сформирована")

    logger.debug("Формируем листы")
    # составляем первый лист
    first_list_svod = pd.concat([tr2_svod.drop(tr2_svod.tail(1).index), oprihod_svod.drop(oprihod_svod.tail(1).index), plr_svod.drop(plr_svod.tail(1).index), ppv_svod.drop(ppv_svod.tail(1).index)]) # объединяем 4 сводных одна под другой
    first_list_svod = pd.pivot_table(first_list_svod, index=['РПС для отчета', 'Собственник для отчета']).reset_index() # составляем сводную из объединенного массива

    if "111 Вид работ ТОРО" in first_list_svod.columns: first_list_svod.drop(columns=["111 Вид работ ТОРО"], inplace=True)
    if "112 Вид работ ТОРО" in first_list_svod.columns: first_list_svod.drop(columns=["112 Вид работ ТОРО"], inplace=True)
    if "113 Вид работ ТОРО" in first_list_svod.columns: first_list_svod.drop(columns=["113 Вид работ ТОРО"], inplace=True)
    if "114 Вид работ ТОРО" in first_list_svod.columns: first_list_svod.drop(columns=["114 Вид работ ТОРО"], inplace=True)

    logger.debug("Отчет. Построена сводная первого листа")

    ppv_for_first_list = pd.pivot_table(ppv_svod, index=['РПС для отчета', 'Собственник для отчета'], values=['Сумма в валюте БЕ'], aggfunc=[np.sum]).reset_index() # составляем сводную для заполнения столбца ППВ
    first_list_svod = pd.merge(first_list_svod, ppv_for_first_list, how='left', on=['РПС для отчета', 'Собственник для отчета']) # заполняем столбец ППВ
    first_list_svod.rename(columns = {('sum', 'Сумма в валюте БЕ') : 'ППВ'}, inplace = True) # переименовываем

    tr2_for_first_list = pd.pivot_table(tr2_svod, index=['РПС для отчета', 'Собственник для отчета'], values=['Сумма в валюте БЕ'], aggfunc=[np.sum]).reset_index() # составляем сводную для заполнения столбца ТОР Списание
    first_list_svod = pd.merge(first_list_svod, tr2_for_first_list, how='left', on=['РПС для отчета', 'Собственник для отчета']) # заполняем столбец ТОР Списание
    first_list_svod.rename(columns = {('sum', 'Сумма в валюте БЕ') : 'ТОР Списание'}, inplace = True) # переименовываем

    oprihod1_for_first_list = pd.pivot_table(oprihod_svod, index=['РПС для отчета', 'Собственник для отчета'], values=['114 Вид работ ТОРО'], aggfunc=[np.sum]).reset_index() # составляем сводную для заполнения столбца ТОР Оприходование
    first_list_svod = pd.merge(first_list_svod, oprihod1_for_first_list, how='left', on=['РПС для отчета', 'Собственник для отчета']) # заполняем столбец ТОР Оприходование
    first_list_svod.rename(columns = {('sum', '114 Вид работ ТОРО') : 'ТОР Оприходование'}, inplace = True) # переименовываем

    plr_for_first_list = pd.pivot_table(plr_svod, index=['РПС для отчета', 'Собственник для отчета'], values=['Сумма в валюте БЕ'], aggfunc=[np.sum]).reset_index() # составляем сводную для заполнения столбца ПЛР Списание
    first_list_svod = pd.merge(first_list_svod, plr_for_first_list, how='left', on=['РПС для отчета', 'Собственник для отчета']) # заполняем столбец ПЛР Списание
    first_list_svod.rename(columns = {('sum', 'Сумма в валюте БЕ') : 'ПЛР Списание'}, inplace = True) # переименовываем

    oprihod2_for_first_list = pd.pivot_table(oprihod_svod, index=['РПС для отчета', 'Собственник для отчета'], values=['111 Вид работ ТОРО'], aggfunc=[np.sum]).reset_index() # составляем сводную для заполнения столбца ПЛР Оприходование
    first_list_svod = pd.merge(first_list_svod, oprihod2_for_first_list, how='left', on=['РПС для отчета', 'Собственник для отчета']) # заполняем столбец ПЛР Оприходование
    first_list_svod.rename(columns = {('sum', '111 Вид работ ТОРО') : 'ПЛР Оприходование'}, inplace = True) # переименовываем
    
    logger.debug("Отчет. Построены 5 сводных для заполнения первого листа")

    first_list_svod = first_list_svod.fillna(0) # заполняем нулями пустоты для возможности суммирования
    first_list_svod['ТОР'] = first_list_svod['ТОР Списание']+first_list_svod['ТОР Оприходование']
    first_list_svod['Затраты по ТОР, ППВ, руб'] = first_list_svod['ТОР']+first_list_svod['ППВ']
    first_list_svod['Затраты по плановым ремонтам, руб'] = first_list_svod['ПЛР Списание']+first_list_svod['ПЛР Оприходование']
    first_list_svod['Суммарные затраты на ремонт, руб'] = first_list_svod['Затраты по ТОР, ППВ, руб']+first_list_svod['Затраты по плановым ремонтам, руб']
    first_list_svod.drop('Сумма в валюте БЕ', axis=1, inplace=True)
    itogo = {'РПС для отчета':'Итого', # формируем строку с итогами
            'ППВ': first_list_svod['ППВ'].sum(), 
            'ТОР Списание': first_list_svod['ТОР Списание'].sum(),
            'ТОР Оприходование': first_list_svod['ТОР Оприходование'].sum(),
            'ПЛР Списание': first_list_svod['ПЛР Списание'].sum(),
            'ПЛР Оприходование': first_list_svod['ПЛР Оприходование'].sum(),
            'ТОР': first_list_svod['ТОР'].sum(),
            'Затраты по ТОР, ППВ, руб': first_list_svod['Затраты по ТОР, ППВ, руб'].sum(),
            'Затраты по плановым ремонтам, руб': first_list_svod['Затраты по плановым ремонтам, руб'].sum(),
            'Суммарные затраты на ремонт, руб': first_list_svod['Суммарные затраты на ремонт, руб'].sum()}
    first_list_svod = first_list_svod.append(itogo, ignore_index=True) # добавляем строку с итогами
    logger.debug("Отчет. Построен первый лист")

    vtbl_oprihod = svod_vtbl_opr(oprihod, spravka_rps_2)
    vtbl_tr2 = svod_vtbl_any(tr2, spravka_rps_2)
    vtbl_plr = svod_vtbl_any(plr, spravka_rps_2)
    vtbl_ppv = svod_vtbl_any(ppv, spravka_rps_2)
    logger.debug("Отчет. Построены сводные ВТБЛ")

    vtbl_oprihod_2 =  for_concat_opr(vtbl_oprihod)
    vtbl_tr2_2 = for_concat_any(vtbl_tr2)
    vtbl_plr_2 = for_concat_any(vtbl_plr)
    vtbl_ppv_2 = for_concat_any(vtbl_ppv)
    logger.debug("Отчет. Подготовлены данные для заполнения массива с const вагоновами ВТБЛ")

    wagons_for_vtbl = pd.read_excel(spravka, 'Вагоны ВТБЛ')
    vtbl_all = itog_for_vtbl(wagons_for_vtbl, vtbl_oprihod_2, vtbl_tr2_2, vtbl_plr_2, vtbl_ppv_2)
    logger.debug("Отчет. заполнена большая таблица ВТБЛ")

    oprihod_for_vtbl_2022 = oprihod[oprihod['Год1']==2022] # для составления сводных по 2022 году фильтруем исходные массивы
    tr2_for_vtbl_2022 = tr2[tr2['Год1']==2022]
    plr_for_vtbl_2022 = plr[plr['Год1']==2022]
    ppv_for_vtbl_2022 = ppv[ppv['Год1']==2022]

    vtbl_oprihod_2022 = svod_vtbl_opr(oprihod_for_vtbl_2022, spravka_rps_2)
    vtbl_tr2_2022 = svod_vtbl_any(tr2_for_vtbl_2022, spravka_rps_2)
    vtbl_plr_2022 = svod_vtbl_any(plr_for_vtbl_2022, spravka_rps_2)    
    vtbl_ppv_2022 = svod_vtbl_any(ppv_for_vtbl_2022, spravka_rps_2)
    logger.debug("Отчет. Построены сводные ВТБЛ 2022")

    vtbl_oprihod_2_2022 = for_concat_opr(vtbl_oprihod_2022)
    vtbl_tr2_2_2022 = for_concat_any(vtbl_tr2_2022)
    vtbl_plr_2_2022 = for_concat_any(vtbl_plr_2022)
    vtbl_ppv_2_2022 = for_concat_any(vtbl_ppv_2022)
    logger.debug("Отчет. Подготовлены данные для заполнения массива с const вагоновами ВТБЛ  2022")

    vtbl_all_2022 = itog_for_vtbl(wagons_for_vtbl, vtbl_oprihod_2_2022, vtbl_tr2_2_2022, vtbl_plr_2_2022, vtbl_ppv_2_2022)
    logger.debug("Отчет. заполнена большая таблица ВТБЛ2022")
    pattern = [['Пролонгированная аренда', 0,0,0,0,0,0,0,0], ['Вагоны к выводу', 0,0,0,0,0,0,0,0], ['ИТОГО', 0,0,0,0,0,0,0,0]] # создаем заготовку таблицы

    itog_vtbl = list_vtbl(pattern, vtbl_all)
    titles = ['Суммарные затраты на ремонт, руб', 'ТОР', 'ППВ', 'Затраты по плановым ремонтам, руб', 'ТОР Списание', 'ТОР Оприходование', 'ПЛР Списание', 'ПЛР Оприходование']
    for i in titles: # заполняем столбцы, перечисленные в titles
        itog_vtbl.loc[0,i] = first_list_svod[first_list_svod['Собственник для отчета']=='ВТБ-Лизинг'][i].sum() - itog_vtbl.loc[1,i] 
        itog_vtbl.loc[2,i] = itog_vtbl.loc[0,i]+itog_vtbl.loc[1,i]

    pattern2 = [['Пролонгированная аренда', 0,0,0,0,0,0,0,0], ['Вагоны к выводу', 0,0,0,0,0,0,0,0], ['Ремонты за 2022', 0,0,0,0,0,0,0,0], ['Ремонты до 2022', 0,0,0,0,0,0,0,0], ['ИТОГО', 0,0,0,0,0,0,0,0]]
    itog_vtbl_2022 = list_vtbl(pattern2, vtbl_all_2022)
    itog_vtbl_2022.loc[0,'ППВ'] = vtbl_ppv_2_2022['Сумма в валюте БЕ'].sum() - itog_vtbl_2022.loc[1,'ППВ']
    itog_vtbl_2022.loc[0,'ТОР Списание'] = vtbl_tr2_2_2022['Сумма в валюте БЕ'].sum() - itog_vtbl_2022.loc[1,'ТОР Списание']
    itog_vtbl_2022.loc[0,'ТОР Оприходование'] = vtbl_oprihod_2_2022['114 Вид работ ТОРО'].sum() - itog_vtbl_2022.loc[1,'ТОР Оприходование']
    itog_vtbl_2022.loc[0,'ПЛР Списание'] = vtbl_plr_2_2022['Сумма в валюте БЕ'].sum() - itog_vtbl_2022.loc[1,'ПЛР Списание']
    itog_vtbl_2022.loc[0,'ПЛР Оприходование'] = -itog_vtbl_2022.loc[1,'ПЛР Оприходование']
    sum_zatr = vtbl_ppv_2_2022['Сумма в валюте БЕ'].sum()+vtbl_tr2_2_2022['Сумма в валюте БЕ'].sum()+vtbl_oprihod_2_2022['114 Вид работ ТОРО'].sum()+vtbl_plr_2_2022['Сумма в валюте БЕ'].sum()
    itog_vtbl_2022.loc[0,'Суммарные затраты на ремонт, руб'] = sum_zatr - itog_vtbl_2022.loc[1,'Суммарные затраты на ремонт, руб']
    itog_vtbl_2022.loc[0,'ТОР'] = itog_vtbl_2022.loc[0,'ТОР Списание']+itog_vtbl_2022.loc[0,'ТОР Оприходование']
    itog_vtbl_2022.loc[0,'Затраты по плановым ремонтам, руб'] = itog_vtbl_2022.loc[0,'ПЛР Списание']+itog_vtbl_2022.loc[0,'ПЛР Оприходование']
    for i in titles:
        itog_vtbl_2022.loc[2,i] = itog_vtbl_2022.loc[0,i]+itog_vtbl_2022.loc[1,i]
        itog_vtbl_2022.loc[3,i] = first_list_svod[first_list_svod['Собственник для отчета']=='ВТБ-Лизинг'][i].sum() - itog_vtbl_2022.loc[2,i]
        itog_vtbl_2022.loc[4,i] = itog_vtbl_2022.loc[2,i]+itog_vtbl_2022.loc[3,i]

    # Выгрузка данных
    month = [ # список месяцев для навзания 1 листа
    'январь',
    'февраль',
    'март',
    'апрель',
    'май',
    'июнь',
    'июль',
    'август',
    'сентябрь',
    'октябрь',
    'ноябрь',
    'декабрь']
    name1 = str(year) + " " + str(month[int(month_start)-1]) + ' привлеченные' # называем первый лист

    with pd.ExcelWriter(Path(spravka.parent, 'Отчет.xlsx'), engine='xlsxwriter') as writer:  
        sheets = [[first_list_svod, name1], [oprihod_svod, 'Оприход'], [tr2_svod, 'ТР-2'], [plr_svod, 'ПЛР'], [ppv_svod, 'ППВ']]
        for i in range(5):
            sheets[i][0].to_excel(writer, sheets[i][1], index=False)
            sheet = writer.sheets[sheets[i][1]]
            cell_format = writer.book.add_format() # создаем формат для строки заголовков
            cell_format1 = writer.book.add_format() # создаем формат для всех столбцов
            cell_format2 = writer.book.add_format() # создаем формат для строки с итогами
            cell_format2.set_bold()
            cell_format2.set_font_size(8)
            cell_format1.set_font_size(8)
            sheet.set_column('A:X', 14, cell_format1)
            cell_format.set_font_color('white')
            cell_format.set_bg_color('#560319')
            cell_format.set_font_size(8)     
            sheet.write_row(0, 0, sheets[i][0].columns, cell_format) # сразу пишем целую строку данных
            for j in range(1000):
                sheet.set_row(j, 10, cell_format1)
            sheet.set_row(len(sheets[i][0]), 10, cell_format2)
        
        itog_vtbl.to_excel(writer, 'ВТБЛ', index=False, startcol=1,startrow=1)
        itog_vtbl_2022.to_excel(writer, 'ВТБЛ', index=False, startcol=1,startrow=10)
        vtbl_all.to_excel(writer, 'Вагоны ВТБЛ', index=False, startcol=0,startrow=1)
        vtbl_oprihod_2.to_excel(writer, 'Вагоны ВТБЛ', index=False, startcol=10,startrow=1)
        vtbl_tr2_2.to_excel(writer, 'Вагоны ВТБЛ', index=False, startcol=18,startrow=1)
        vtbl_plr_2.to_excel(writer, 'Вагоны ВТБЛ', index=False, startcol=24,startrow=1)
        vtbl_ppv_2.to_excel(writer, 'Вагоны ВТБЛ', index=False, startcol=32,startrow=1)
        vtbl_all_2022.to_excel(writer, 'Вагоны ВТБЛ 2022', index=False, startcol=0,startrow=1)
        vtbl_oprihod_2_2022.to_excel(writer, 'Вагоны ВТБЛ 2022', index=False, startcol=10,startrow=1)
        vtbl_tr2_2_2022.to_excel(writer, 'Вагоны ВТБЛ 2022', index=False, startcol=18,startrow=1)
        vtbl_plr_2_2022.to_excel(writer, 'Вагоны ВТБЛ 2022', index=False, startcol=24,startrow=1)
        vtbl_ppv_2_2022.to_excel(writer, 'Вагоны ВТБЛ 2022', index=False, startcol=32,startrow=1)

        sheets = [[vtbl_all, 'Вагоны ВТБЛ'], [vtbl_oprihod_2, 'Вагоны ВТБЛ'], [vtbl_tr2_2, 'Вагоны ВТБЛ'], [vtbl_plr_2, 'Вагоны ВТБЛ'], [vtbl_ppv_2, 'Вагоны ВТБЛ'],
            [vtbl_all_2022, 'Вагоны ВТБЛ 2022'], [vtbl_oprihod_2_2022, 'Вагоны ВТБЛ 2022'], [vtbl_tr2_2_2022, 'Вагоны ВТБЛ 2022'], [vtbl_plr_2_2022, 'Вагоны ВТБЛ 2022'], [vtbl_ppv_2_2022, 'Вагоны ВТБЛ 2022']]
        for i in range(2):
            sheet = writer.sheets[sheets[i*5][1]]        
            cell_format1 = writer.book.add_format() # создаем формат для остальных строк
            cell_format1.set_font_size(8) # устанавливаем размер шрифта для всех строк
            for j in range(100000):
                sheet.set_row(j, 10, cell_format1) # устанавливаем высоту строки для всех строк
            cell_format = writer.book.add_format() # создаем формат для первой строки
            cell_format.set_font_color('white')
            cell_format.set_bg_color('#560319')
            cell_format.set_font_size(8)
            sheet.write_row(1, 0, sheets[i*5+0][0].columns, cell_format) # записываем формат в строку заголовков
            sheet.write_row(1, 10, sheets[i*5+1][0].columns, cell_format) 
            sheet.write_row(1, 18, sheets[i*5+2][0].columns, cell_format) 
            sheet.write_row(1, 24, sheets[i*5+3][0].columns, cell_format) 
            sheet.write_row(1, 32, sheets[i*5+4][0].columns, cell_format) 
            sheet.write(0,10,'Оприход') # прописываем обозначения для сводных таблиц
            sheet.write(0,18,'Списание в ТР-2')
            sheet.write(0,24,'Списание в ПЛР')
            sheet.write(0,32,'ППВ')
        
        sheet = writer.sheets['ВТБЛ']
        sheet.set_column(1, 1, 17, cell_format1)
        for j in range(100000):
            sheet.set_row(j, 10, cell_format1) # устанавливаем высоту строки для всех строк
        sheet.write_row(1, 1, itog_vtbl.columns, cell_format) # записываем формат в первую строку
        sheet.write_row(10, 1, itog_vtbl_2022.columns, cell_format) # записываем формат в 10-ю строку


