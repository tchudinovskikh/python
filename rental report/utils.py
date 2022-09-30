import pandas as pd
import numpy as np
from typing import List, Tuple
import cx_Oracle
import sqlalchemy
import logging

logger1 = logging.getLogger("my_log_rent")
logger1.info("from data_processing.py")

def cutting(df_zco):

    ''' 
    Функция для фильтрации общего массива данных на отдельные датафреймы: Оприход, ПЛР, ТР-2 и ППВ 
    
    '''

    excess_mvz = [1010042044, 1010042046, 1010042047,
    1010042048, 1010042147, 1010042148,
    1010043017, 1010043019, 1010082044,
    1010082046, 1010082047, 1010082048,
    1010092000, 1010092046, 1010092047,
    1010092048, 1010094400, 1010103018,
    1010132048, 1010134400, 1010991001,
    1010991002, 1010991004, 1010991009,
    1010991011, 1010991014, 1010991015]
    oprihod = df_zco[((df_zco['Счет']==9110302100) | (df_zco['Счет']==9110303100)) & ((df_zco['Вид работ ТОРО']==111) |
                                                                                  (df_zco['Вид работ ТОРО']==112) |
                                                                                  (df_zco['Вид работ ТОРО']==113) |
                                                                                  (df_zco['Вид работ ТОРО']==114) |
                                                                                  (df_zco['Вид работ ТОРО']==121))]
    plr = df_zco[(((df_zco['Счет']==3661200001) & (df_zco['КорреспСч']!=804100011)) | 
              (df_zco['Счет']==3101001000) | 
              ((df_zco['Счет']==3661100001) & (df_zco['КорреспСч']!=804100011)) | 
              ((df_zco['Счет']==3661300001) & (df_zco['КорреспСч']!=804100011)) | 
              (df_zco['Счет']==3502001000) | 
              (df_zco['Счет']==3101002000) | 
              (df_zco['Счет']==3502001100) | 
              (df_zco['Счет']==3502002000)) & ((df_zco['Вид работ ТОРО']==111) |
                                               (df_zco['Вид работ ТОРО']==112) |
                                               (df_zco['Вид работ ТОРО']==190))]
    plr = plr[~plr['МВЗ'].isin(excess_mvz)]
    tr2 = df_zco[((df_zco['Счет']==3502001000) |
              (df_zco['Счет']==3501009000) |
              (df_zco['Счет']==3101001000) |
              (df_zco['Счет']==3101002000) |
              (df_zco['Счет']==3502002000)) & (df_zco['Вид работ ТОРО']==114)]
    tr2 = tr2[~tr2['МВЗ'].isin(excess_mvz)]
    ppv = df_zco[((df_zco['Счет']==3501009000) |
              (df_zco['Счет']==3501024000)) & ((df_zco['Вид работ ТОРО']==113) |
                                               (df_zco['Вид работ ТОРО']==122)) & ((df_zco['Статья отчета - текст']=='3.5.1.1 Коммерческая подготовка') | 
                                                                                 (df_zco['Статья отчета - текст']=='3.5.1.2 Техническая подготовка'))]
    return oprihod, plr, tr2, ppv


def svod_vtbl_opr(oprihod, spravka_rps_2):
    '''
    Функция для составления сводной по оприходу для листа "Вагоны ВТБЛ". 
    Функция фильтрует датафрейм Оприход и формирует сводную.
    В случае, если после фильтрации Оприхода остался пустой датафрейм, то функция составляет пустой шаблон сводной.

    '''
    oprihod_for_vtbl_0 = oprihod[(oprihod['Собственник УА'].notnull()) &
                                 (oprihod['Статья отчета - текст'] != '3.2.1.1.2.2. Услуги по ТОР вагонов (арендованные)') &
                                 (oprihod['Статья отчета - текст'] != '3.3.2 Пропарка на арендованных ППС')].copy() # выбираем строки, где есть собственник, чтобы следующая строчка сработала
    oprihod_for_vtbl_1 = oprihod_for_vtbl_0[oprihod_for_vtbl_0['Собственник УА'].str.contains(r'\bВТБ')] # выбираем осбтсвенников, в названии которых есть ВТБ
    if oprihod_for_vtbl_1.shape[0] == 0:
        vtbl1 = pd.DataFrame(columns=['Род ПС', 
        'Номер вагона',
        '№ договора',
        '114 Вид работ ТОРО',
        'Сумма в валюте БЕ'])
    else:
        vtbl1 = pd.pivot_table(oprihod_for_vtbl_1, index=['Род ПС.1','№ договора', 'Номер вагона'], columns=['Вид работ ТОРО'], values=['Сумма в валюте БЕ'], aggfunc=[np.sum], margins=True).reset_index() # формируем необходимую сводную\
        logger1.debug(f"Количество строк в сводной оприхода до справочного ВПР: {vtbl1.shape[0]}")
        vtbl1 = pd.merge(vtbl1, spravka_rps_2, how='left', left_on='Род ПС.1', right_on='РПС') # ВПР РПС для отчета, после этого уровни загаловков датафрейма схлопываются
        logger1.debug(f"Количество строк в сводной оприхода после справочного ВПР: {vtbl1.shape[0]}")        
        vtbl1.rename(columns = {('Род ПС.1', '', '') : 'Род ПС', 
                                    ('Номер вагона', '', '') : 'Номер вагона',
                                    ('№ договора', '', '') : '№ договора',
                                    ('sum', 'Сумма в валюте БЕ', 114.0) : '114 Вид работ ТОРО',
                                    ('sum', 'Сумма в валюте БЕ', 'All') : 'Сумма в валюте БЕ'}, inplace = True) # переименовываем заголовки после схлопывания 
        vtbl1.drop(['РПС', 'РПС для отчета'], axis=1, inplace=True) # удаляем лишний столбец после ВПР
    return vtbl1

def svod_vtbl_any(tr2, spravka_rps_2):
    '''
    Функция для составления сводной по ТР-2, ПЛР и ППВ для листа "Вагоны ВТБЛ". 
    Функция фильтрует датафреймы и формирует сводную.
    В случае, если после фильтрации массивов остался пустой датафрейм, то функция составляет пустой шаблон сводной.

    '''
    tr2_for_vtbl_0 = tr2[(tr2['Собственник УА'].notnull()) &
                        (tr2['Номер счета'] != 'Работы по ремонту вагонов, неотфактурованные')].copy()
    tr2_for_vtbl_1 = tr2_for_vtbl_0[tr2_for_vtbl_0['Собственник УА'].str.contains(r'\bВТБ')]
    logger1.debug(f"Количество отфлитрованных строк: {tr2_for_vtbl_1.shape[0]}") 
    if tr2_for_vtbl_1.shape[0] == 0:
        vtbl2 = pd.DataFrame(columns=['Род ПС', 
        'Номер вагона',
        '№ договора',
        'Сумма в валюте БЕ'])
    else:
        vtbl2 = pd.pivot_table(tr2_for_vtbl_1, index=['Род ПС.1', '№ договора', 'Номер вагона'], values=['Сумма в валюте БЕ'], aggfunc=[np.sum], margins=True).reset_index()
        logger1.debug(f"Количество строк в сводной НЕ оприхода до справочного ВПР: {vtbl2.shape[0]}")
        vtbl2 = pd.merge(vtbl2, spravka_rps_2, how='left', left_on='Род ПС.1', right_on='РПС') # ВПР РПС для отчета, после этого уровни загаловков датафрейма схлопываются
        logger1.debug(f"Количество строк в сводной НЕ оприхода после справочного ВПР: {vtbl2.shape[0]}")
        vtbl2.rename(columns = {('Род ПС.1', '') : 'Род ПС', 
                                    ('Номер вагона', '') : 'Номер вагона',
                                    ('№ договора', '') : '№ договора',
                                    ('sum', 'Сумма в валюте БЕ') : 'Сумма в валюте БЕ'}, inplace = True) # переименовываем заголовки после схлопывания 
        vtbl2.drop(['РПС', 'РПС для отчета'], axis=1, inplace=True) # удаляем лишний столбец после ВПР
    return vtbl2

def for_concat_opr(vtbl1):
    '''
    Функция удаляет строчку с итогами у составленной сводной таблицы для того, чтобы соединить сводные в единый массив

    '''
    if vtbl1.shape[0] != 0:
        vtbl1.drop(vtbl1.tail(1).index, inplace=True) # удаляем строку с итогам
        vtbl1['Номер вагона'] = vtbl1['Номер вагона'].astype('int64')
    return vtbl1

def for_concat_any(vtbl2):
    '''
    Функция удаляет строчку с итогами у составленной сводной таблицы

    '''
    if vtbl2.shape[0] != 0:
        vtbl2.drop(vtbl2.tail(1).index, inplace=True) # удаляем строку с итогам
        vtbl2['Номер вагона'] = vtbl2['Номер вагона'].astype('int64')
    return vtbl2

def itog_for_vtbl(wagons_for_vtbl, vtbl1_2, vtbl2_2, vtbl3_2, vtbl4_2):
    '''
    Функция последовательно ВПРит к заранее подготовленному списку вагонов составленные для листа "Вагоны ВТБЛ" сводные таблицы

    '''
    logger1.debug(f"Количество строк в листе vtbl до ВПР со сводной оприхода: {wagons_for_vtbl.shape[0]}")
    vtbl = pd.merge(wagons_for_vtbl, vtbl1_2, how='left', on='Номер вагона')
    logger1.debug(f"Количество строк в листе vtbl после ВПР со сводной оприхода: {vtbl.shape[0]}")
    vtbl.drop(['114 Вид работ ТОРО', 'Род ПС', '№ договора'], axis=1, inplace=True) # удаляем лишний столбец после ВПР
    vtbl.rename(columns = {'Сумма в валюте БЕ' : 'ТОР Оприходование'}, inplace = True) # переименовываем

    logger1.debug(f"Количество строк в листе vtbl до ВПР со сводной оприхода 2: {vtbl.shape[0]}")
    vtbl = pd.merge(vtbl, vtbl1_2, how='left', on='Номер вагона')
    logger1.debug(f"Количество строк в листе vtbl после ВПР со сводной оприхода 2: {vtbl.shape[0]}")
    vtbl.drop(['Сумма в валюте БЕ', 'Род ПС', '№ договора'], axis=1, inplace=True) # удаляем лишний столбец после ВПР
    vtbl = vtbl.fillna(0)
    vtbl['ПЛР Оприходование'] = vtbl['ТОР Оприходование']+vtbl['114 Вид работ ТОРО']
    vtbl.drop('114 Вид работ ТОРО', axis=1, inplace=True)

    logger1.debug(f"Количество строк в листе vtbl до ВПР со сводной тр2: {vtbl.shape[0]}")
    vtbl = pd.merge(vtbl, vtbl3_2, how='left', on='Номер вагона')
    logger1.debug(f"Количество строк в листе vtbl после ВПР со сводной тр2: {vtbl.shape[0]}")
    vtbl.drop(['Род ПС', '№ договора'], axis=1, inplace=True) # удаляем лишний столбец после ВПР
    vtbl.rename(columns = {'Сумма в валюте БЕ' : 'Затраты по плановым ремонтам, руб'}, inplace = True) # переименовываем

    logger1.debug(f"Количество строк в листе vtbl до ВПР со сводной плр: {vtbl.shape[0]}")
    vtbl = pd.merge(vtbl, vtbl2_2, how='left', on='Номер вагона')
    logger1.debug(f"Количество строк в листе vtbl после ВПР со сводной плр: {vtbl.shape[0]}")
    vtbl.drop(['Род ПС', '№ договора'], axis=1, inplace=True) # удаляем лишний столбец после ВПР
    vtbl.rename(columns = {'Сумма в валюте БЕ' : 'ТОР'}, inplace = True) # переименовываем

    if vtbl4_2.shape[0] != 0:
        vtbl4_3 = pd.pivot_table(vtbl4_2, index='Номер вагона').reset_index()
        logger1.debug(f"Количество строк в листе vtbl до ВПР со сводной ппв: {vtbl.shape[0]}")
        vtbl = pd.merge(vtbl, vtbl4_3, how='left', on='Номер вагона')
        logger1.debug(f"Количество строк в листе vtbl после ВПР со сводной ппв: {vtbl.shape[0]}")
        vtbl.rename(columns = {'Сумма в валюте БЕ' : 'ППВ'}, inplace = True) # переименовываем
    else:
        vtbl['ППВ'] = 0

    vtbl = vtbl.fillna(0)
    vtbl['Суммарные затраты на ремонт, руб'] = vtbl['ТОР Оприходование']+vtbl['ПЛР Оприходование']+vtbl['Затраты по плановым ремонтам, руб']+vtbl['ТОР']+vtbl['ППВ']
    
    return vtbl

def list_vtbl(pattern, vtbl_all):
    '''
    Функция по заранее подготовленному шаблона pattern заполняет таблицы с итогами отчета по ВТБЛ
    Происходит последовательная запись в ячейки шаблона необходивых расчетных значений
    
    '''
    itog_vtbl = pd.DataFrame(pattern, columns=['Втб Лизинг', 'Суммарные затраты на ремонт, руб', 'ТОР', 'ППВ', 'Затраты по плановым ремонтам, руб', 'ТОР Списание', 'ТОР Оприходование', 'ПЛР Списание', 'ПЛР Оприходование'])
    itog_vtbl.loc[1,'Суммарные затраты на ремонт, руб'] = vtbl_all['Суммарные затраты на ремонт, руб'].drop(vtbl_all.tail(1).index).sum() # суммируем значения сводной без последней строки "итого"
    itog_vtbl.loc[1,'ППВ'] = vtbl_all['ППВ'].drop(vtbl_all.tail(1).index).sum()
    itog_vtbl.loc[1,'ТОР Списание'] = vtbl_all['ТОР'].drop(vtbl_all.tail(1).index).sum()
    itog_vtbl.loc[1,'ТОР Оприходование'] = vtbl_all['ТОР Оприходование'].drop(vtbl_all.tail(1).index).sum()
    itog_vtbl.loc[1,'ПЛР Списание'] = vtbl_all['Затраты по плановым ремонтам, руб'].drop(vtbl_all.tail(1).index).sum()
    itog_vtbl.loc[1,'ПЛР Оприходование'] = vtbl_all['ПЛР Оприходование'].drop(vtbl_all.tail(1).index).sum()
    itog_vtbl.loc[1,'ТОР'] = itog_vtbl.loc[1,'ТОР Списание']+itog_vtbl.loc[1,'ТОР Оприходование']
    itog_vtbl.loc[1,'Затраты по плановым ремонтам, руб'] = itog_vtbl.loc[1,'ПЛР Списание']+itog_vtbl.loc[1,'ПЛР Оприходование']
    return itog_vtbl 

def svod_any(any, spravka_rps_2, kontragent):
    any_svod = any[any['Номер счета'] != 'Работы по ремонту вагонов, неотфактурованные'].copy() # создаем копию для свода
    any_svod = pd.pivot_table(any_svod, index=['Род ПС.1','Собственник УА', '№ договора'], values=['Сумма в валюте БЕ'], aggfunc=[np.sum], margins=True) # создаем сводную
    any_svod = any_svod.reset_index() # превращаем сводную в датафрейм
    any_svod = pd.merge(any_svod, spravka_rps_2, how='left', left_on='Род ПС.1', right_on='РПС') # ВПР РПС для отчета, после этого уровни загаловков датафрейма схлопываются
    any_svod.rename(columns = {('Род ПС.1', '') : 'Род ПС', 
                                    ('Собственник УА', '') : 'Собственник',
                                    ('№ договора', '') : '№ договора',
                                    ('sum', 'Сумма в валюте БЕ') : 'Сумма в валюте БЕ'}, inplace = True) # переименовываем заголовки после схлопывания 
    any_svod.drop('РПС', axis=1, inplace=True) # удаляем лишний столбец после ВПР
    any_svod_without_last_row = pd.merge(any_svod.drop(any_svod.tail(1).index), kontragent, how='left', left_on='№ договора', right_on='Номер договора') # ВПР "собственника для отчета" по номеру договора
    any_svod = pd.concat([any_svod_without_last_row, any_svod.tail(1)]) # возвращаем строку с итогами
    any_svod.loc[(any_svod['Собственник'].str.contains(r'\bВТБ')) | (any_svod['Собственник'].str.contains(r'\bФинансБизнесГрупп')), 'Собственник для отчета'] = 'ВТБ-Лизинг'
    any_svod.loc[any_svod['Собственник для отчета'].isnull(), 'Собственник для отчета'] = any_svod['Собственник']
    any_svod.drop('Номер договора', axis=1, inplace=True) # удаляем лишний столбец после ВПР
    any_svod.loc[any_svod['РПС для отчета'].isnull(), 'РПС для отчета'] = any_svod['Род ПС']
    return any_svod

def svod_oprihod(oprihod, spravka_rps_2, kontragent):
    oprihod_svod = oprihod.copy() # создаем копию для свода
    oprihod_svod = pd.pivot_table(oprihod_svod, index=['Род ПС.1','Собственник УА', '№ договора'], columns=['Вид работ ТОРО'], values=['Сумма в валюте БЕ'], aggfunc=[np.sum], margins=True) # создаем сводную
    oprihod_svod = oprihod_svod.reset_index() # превращаем сводную в датафрейм
    oprihod_svod = pd.merge(oprihod_svod, spravka_rps_2, how='left', left_on='Род ПС.1', right_on='РПС') # ВПР РПС для отчета, после этого уровни загаловков датафрейма схлопываются
    oprihod_svod.rename(columns = {('Род ПС.1', '', '') : 'Род ПС', 
                                ('Собственник УА', '', '') : 'Собственник',
                                ('№ договора', '', '') : '№ договора',
                                ('sum', 'Сумма в валюте БЕ', 111.0) : '111 Вид работ ТОРО',
                                ('sum', 'Сумма в валюте БЕ', 112.0) : '112 Вид работ ТОРО',
                                ('sum', 'Сумма в валюте БЕ', 113.0) : '113 Вид работ ТОРО',
                                ('sum', 'Сумма в валюте БЕ', 114.0) : '114 Вид работ ТОРО',
                                ('sum', 'Сумма в валюте БЕ', 121.0) : '121 Вид работ ТОРО',
                                ('sum', 'Сумма в валюте БЕ', 'All') : 'Сумма в валюте БЕ'}, inplace = True) # переименовываем заголовки после схлопывания 
    oprihod_svod.drop('РПС', axis=1, inplace=True) # удаляем лишний столбец после ВПР
    oprihod_svod_without_last_row = pd.merge(oprihod_svod.drop(oprihod_svod.tail(1).index), kontragent, how='left', left_on='№ договора', right_on='Номер договора') # ВПР "собственника для отчета" по номеру договора
    oprihod_svod = pd.concat([oprihod_svod_without_last_row, oprihod_svod.tail(1)]) # возвращаем строку с итогами
    oprihod_svod.loc[(oprihod_svod['Собственник'].str.contains(r'\bВТБ')) | (oprihod_svod['Собственник'].str.contains(r'\bФинансБизнесГрупп')), 'Собственник для отчета'] = 'ВТБ-Лизинг'
    oprihod_svod.loc[oprihod_svod['Собственник для отчета'].isnull(), 'Собственник для отчета'] = oprihod_svod['Собственник']    
    oprihod_svod.drop('Номер договора', axis=1, inplace=True) # удаляем лишний столбец после ВПР
    oprihod_svod.loc[oprihod_svod['РПС для отчета'].isnull(), 'РПС для отчета'] = oprihod_svod['Род ПС']
    return oprihod_svod

# def get_ownership_df(wagnums_lst: List[Tuple[int, pd.Timestamp]]) -> pd.DataFrame:
#     '''
#     Функция, используя SQL-запрос, обращается к БД для определения контрагента, у которого ПГК арендует вагон
#     Поиск контрагента производится на конкретную дату
    
#     '''
#     clause_list = []
#     for wagnum, rep_date in wagnums_lst:
#         rep_date_str = rep_date.date().isoformat()
#         clause_list.append(f"SELECT {wagnum} wagnum, DATE '{rep_date_str}' report_dt FROM dual")
#     cte_clause = "\nUNION\n".join(clause_list)

#     owner_df = pd.read_sql(f"""
#     WITH wag_cte AS (
#         {cte_clause}
#     )
#     SELECT
#         t.wagnum, t.owner, 
#         DECODE(
#             t.owner,
#             'ОАО ПГК', t.owner,
#             'НЛМК', t.owner,
#             'Арендодатель'
#         ) ownership,
#         t.beg_date, t.end_date, t.report_dt
#     FROM (
#         SELECT 
#             wsh.WAGNUM, wsh.BEG_DATE, 
#             NVL(LEAD(wsh.BEG_DATE) OVER(
#                 PARTITION BY wsh.WAGNUM ORDER BY wsh.BEG_DATE, wsh.CHANGE_RECORD_CREATION_DATE DESC
#             ), DATE '9999-12-31') end_date,
#             w.report_dt,
#             DECODE(
#                 bp.SHORT_NAME, 
#                 'ПАО "ПГК"', 'ОАО ПГК', 
#                 'Центральный аппарат ПАО "ПГК"', 'ОАО ПГК',
#                 NULL, 'ОАО ПГК',
#                 'ПАО "НЛМК"', 'НЛМК',
# --                'АО "НЛМК-УРАЛ"', 'НЛМК',
#                 bp.SHORT_NAME
#             ) owner
#         FROM sap.WAG_SOBST_HIST wsh
#         LEFT JOIN sap.BUSINESS_PARTNER bp 
#             ON wsh.WAG_OWNER_ID = bp.BP_ID
#         INNER JOIN wag_cte w
#             ON wsh.WAGNUM = w.wagnum
#     ) t
#     WHERE
#         t.report_dt BETWEEN t.BEG_DATE and t.END_DATE
#     """, con=sqlalchemy.create_engine())

#     return owner_df 

def get_ownership_df(wagnums_lst: List[Tuple[int, pd.Timestamp]]) -> pd.DataFrame:
    clause_list = []
    for wagnum, rep_date in wagnums_lst:
        rep_date_str = rep_date.date().isoformat()
        clause_list.append(f"SELECT {wagnum} wagnum, DATE '{rep_date_str}' report_dt FROM dual")
    cte_clause = "\nUNION\n".join(clause_list)

    owner_df = pd.read_sql(f"""
    WITH wag_rep AS (
        {cte_clause}
    )
    SELECT 
        t.wagnum, t.contragent owner
        , DECODE(t.contragent, 'ПАО "ПГК"', t.contragent, 'Арендодатель') ownership
        , t.beg_date, t.end_date, t.report_dt
    FROM (
        SELECT
            wr.wagnum, wr.report_dt
            , NVL(c.BEGIN_DATE, DATE '1000-01-01')  BEG_DATE
            , NVL(c.END_DATE, DATE '9999-12-31')    END_DATE
            , NVL(bp.SHORT_NAME, 'ПАО "ПГК"')       contragent
            , ROW_NUMBER() OVER(PARTITION BY wr.wagnum, wr.report_dt ORDER BY c.BEGIN_DATE DESC) rn
        FROM sap.RENT_ACT_WAG aw
        INNER JOIN sap.RENT_ACT ra ON aw.RENT_ACT_ID = ra.RENT_ACT_ID
        INNER JOIN sap.CONTRACT c ON ra.CONTRACT_ID = c.CONTRACT_ID
        INNER JOIN sap.BUSINESS_PARTNER bp ON c.BP_ID = bp.BP_ID 
        RIGHT JOIN wag_rep wr ON aw.WAGNUM = wr.wagnum
        WHERE NVL(ra.TYPE_ID, 'K1') = 'K1' 
            AND wr.report_dt BETWEEN NVL(c.BEGIN_DATE, DATE '1000-01-01') AND NVL(c.END_DATE, DATE '9999-12-31')
    ) t
    WHERE t.rn = 1
    -- ORDER BY t.report_dt DESC, t.wagnum, t.BEG_DATE DESC
    """, con=sqlalchemy.create_engine())

    # logger.info(f"owner_df.shape: {owner_df.shape}")

    return owner_df