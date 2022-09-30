import win32com.client
from SapGuiLibrary.SapGuiLibrary import SapGuiLibrary
from pathlib import Path
import pythoncom #нужно для компиляции

def sap_start(transaction):

    '''
    Функция выполняет подключение к транзации SAP'a
    
    '''

    pythoncom.CoInitialize()
    sap = SapGuiLibrary()
    sap.connect_to_session()
    sap.open_connection("31. S4P - Продуктивная система")

    sap.session.findById("wnd[0]").maximize()
    sap.session.findById("wnd[0]/tbar[0]/okcd").text = transaction
    sap.session.findById("wnd[0]/tbar[0]/btn[0]").press()
    sap.session.findById("wnd[0]").sendVKey(0)
    
    return sap

def sap_exit(file_path):

    '''
    Функция закрывает выгруженный из SAP'a excel-документ

    '''

    FileName = str(file_path)
    office = win32com.client.DispatchEx("Excel.Application") # работаем с эксель
    wb2 = win32com.client.GetObject(FileName)
    wb2.Close()
    office.Quit() 

def sap_main_zco(df_accounts, year, month_start, month_finish, file_path, transaction):

    '''
    Функция формирует основную выгрузку из ZCO_LINE_ITEMS 
    
    '''

    sap = sap_start(transaction)

    sap.session.findById("wnd[0]/usr/txtS_GJAHR-LOW").text = year
    sap.session.findById("wnd[0]/usr/txtS_POPER-LOW").text = month_start
    sap.session.findById("wnd[0]/usr/txtS_POPER-HIGH").text = month_finish
    sap.session.findById("wnd[0]/usr/ctxtP_HIER").text = "ma31"

    df_accounts.to_clipboard(index=False,header=False)
    sap.session.findById("wnd[0]/usr/btn%_SP$00001_%_APP_%-VALU_PUSH").press()
    sap.session.findById("wnd[1]/tbar[0]/btn[24]").press()
    sap.session.findById("wnd[1]/tbar[0]/btn[8]").press()

    sap.session.findById("wnd[0]/usr/ctxtSP$00038-LOW").text = "coin"
    sap.session.findById("wnd[0]/usr/ctxt%ALVL").setFocus()
    sap.session.findById("wnd[0]/usr/ctxt%ALVL").caretPosition = 0
    sap.session.findById("wnd[0]").sendVKey(4)
    sap.session.findById("wnd[1]/usr/ssubD0500_SUBSCREEN:SAPLSLVC_DIALOG:0501/cntlG51_CONTAINER/shellcont/shell").currentCellRow = 40
    sap.session.findById("wnd[1]/usr/ssubD0500_SUBSCREEN:SAPLSLVC_DIALOG:0501/cntlG51_CONTAINER/shellcont/shell").firstVisibleRow = 33
    sap.session.findById("wnd[1]/usr/ssubD0500_SUBSCREEN:SAPLSLVC_DIALOG:0501/cntlG51_CONTAINER/shellcont/shell").selectedRows = "40"
    sap.session.findById("wnd[1]/usr/ssubD0500_SUBSCREEN:SAPLSLVC_DIALOG:0501/cntlG51_CONTAINER/shellcont/shell").clickCurrentCell()
    sap.session.findById("wnd[0]/tbar[1]/btn[8]").press() # ЗАПУСКАЕМ ВЫГРУЗКУ

    sap.session.findById("wnd[0]/tbar[1]/btn[32]").press() # УДАЛЯЕМ ИТОГОВЫЕ СУММЫ
    sap.session.findById("wnd[1]/usr/tabsG_TS_ALV/tabpALV_M_R1/ssubSUB_CONFIGURATION:SAPLSALV_CUL_COLUMN_SELECTION:0620/cntlCONTAINER2_LAYO/shellcont/shell").setCurrentCell(6,"DO_SUM")
    sap.session.findById("wnd[1]/usr/tabsG_TS_ALV/tabpALV_M_R1/ssubSUB_CONFIGURATION:SAPLSALV_CUL_COLUMN_SELECTION:0620/cntlCONTAINER2_LAYO/shellcont/shell").selectedRows = "6"
    sap.session.findById("wnd[1]/usr/tabsG_TS_ALV/tabpALV_M_R1/ssubSUB_CONFIGURATION:SAPLSALV_CUL_COLUMN_SELECTION:0620/cntlCONTAINER2_LAYO/shellcont/shell").clickCurrentCell()
    sap.session.findById("wnd[1]/usr/tabsG_TS_ALV/tabpALV_M_R1/ssubSUB_CONFIGURATION:SAPLSALV_CUL_COLUMN_SELECTION:0620/cntlCONTAINER2_LAYO/shellcont/shell").currentCellRow = 7
    sap.session.findById("wnd[1]/usr/tabsG_TS_ALV/tabpALV_M_R1/ssubSUB_CONFIGURATION:SAPLSALV_CUL_COLUMN_SELECTION:0620/cntlCONTAINER2_LAYO/shellcont/shell").selectedRows = "7"
    sap.session.findById("wnd[1]/usr/tabsG_TS_ALV/tabpALV_M_R1/ssubSUB_CONFIGURATION:SAPLSALV_CUL_COLUMN_SELECTION:0620/cntlCONTAINER2_LAYO/shellcont/shell").clickCurrentCell()
    sap.session.findById("wnd[1]/tbar[0]/btn[0]").press()

    sap.session.findById("wnd[0]/usr/cntlGRID1/shellcont/shell").contextMenu()
    sap.session.findById("wnd[0]/usr/cntlGRID1/shellcont/shell").selectContextMenuItem("&XXL")

    sap.session.findById("wnd[1]/tbar[0]/btn[0]").press()
    sap.session.findById("wnd[1]/usr/ctxtDY_PATH").text = str(file_path.parent.absolute())
    sap.session.findById("wnd[1]/usr/ctxtDY_FILENAME").text = str(file_path.name)

    sap.session.findById("wnd[1]/tbar[0]/btn[11]").press()
    sap.session.findById("wnd[0]").close()
    sap.session.findById("wnd[1]/usr/btnSPOP-OPTION1").press()

    sap_exit(file_path)

def sap_wagon_numbers_iw38(toro_order, file_path, transaction):

    '''
    Функция по ТОРО-заказам выполняет поиск номеров вагонов в IW38
    
    '''

    sap = sap_start(transaction)

    sap.session.findById("wnd[0]/usr/ctxtDATUV").text = "22.12.2018"
    sap.session.findById("wnd[0]/usr/ctxtDATUV").setFocus
    sap.session.findById("wnd[0]/usr/ctxtDATUV").caretPosition = 10
    toro_order.to_clipboard(index=False,header=False)
    sap.session.findById("wnd[0]/usr/btn%_AUFNR_%_APP_%-VALU_PUSH").press()
    sap.session.findById("wnd[1]/tbar[0]/btn[24]").press()
    sap.session.findById("wnd[1]/tbar[0]/btn[8]").press()

    sap.session.findById("wnd[0]/usr/ctxtVARIANT").setFocus()
    sap.session.findById("wnd[0]/usr/ctxtVARIANT").caretPosition = 14
    sap.session.findById("wnd[0]").sendVKey(4)
    sap.session.findById("wnd[1]/usr/lbl[1,5]").setFocus()
    sap.session.findById("wnd[1]/usr/lbl[1,5]").caretPosition = 2
    sap.session.findById("wnd[1]").sendVKey(2)

    sap.session.findById("wnd[0]/tbar[1]/btn[8]").press() # ЗАПУСК

    sap.session.findById("wnd[0]/usr/cntlGRID1/shellcont/shell").contextMenu()
    sap.session.findById("wnd[0]/usr/cntlGRID1/shellcont/shell").selectContextMenuItem("&XXL")
    sap.session.findById("wnd[1]/tbar[0]/btn[0]").press()

    sap.session.findById("wnd[1]/usr/ctxtDY_PATH").text = str(file_path.parent.absolute())
    sap.session.findById("wnd[1]/usr/ctxtDY_FILENAME").text = str(file_path.name)
    sap.session.findById("wnd[1]/tbar[0]/btn[11]").press()
    sap.session.findById("wnd[0]").close()
    sap.session.findById("wnd[1]/usr/btnSPOP-OPTION1").press()

    sap_exit(file_path)

def sap_RPS_zpm(wagons_without_RPS, file_path, transaction):

    '''
    Функция по номерам вагонов выполняет поиск их РПС в zpm_pgk_erv
    
    '''

    sap = sap_start(transaction)

    wagons_without_RPS.to_clipboard(index=False,header=False) # копируем в буфер обмена

    sap.session.findById("wnd[0]/usr/ssubGV_SUBSCR_STATPARAM:ZPM_PGK_ERV:1002/btn%_S_WAGNUM_%_APP_%-VALU_PUSH").press()
    sap.session.findById("wnd[1]/tbar[0]/btn[24]").press()
    sap.session.findById("wnd[1]/tbar[0]/btn[8]").press()
    sap.session.findById("wnd[0]/shellcont[1]/shell").expandNode("WG_PGK")
    sap.session.findById("wnd[0]/shellcont[1]/shell").selectItem("14","1")
    sap.session.findById("wnd[0]/shellcont[1]/shell").ensureVisibleHorizontalItem("14","1")
    sap.session.findById("wnd[0]/shellcont[1]/shell").topNode = "WG_PGK"
    sap.session.findById("wnd[0]/shellcont[1]/shell").changeCheckbox("14","1","true")
    sap.session.findById("wnd[0]/shellcont[1]/shell").collapseNode("WG_PGK")
    sap.session.findById("wnd[0]/shellcont[1]/shell").expandNode("GVC")
    sap.session.findById("wnd[0]/shellcont[1]/shell").selectItem("114","1")
    sap.session.findById("wnd[0]/shellcont[1]/shell").ensureVisibleHorizontalItem("114","1")
    sap.session.findById("wnd[0]/shellcont[1]/shell").topNode = "GVC"
    sap.session.findById("wnd[0]/shellcont[1]/shell").changeCheckbox("114","1","true")
    sap.session.findById("wnd[0]/shellcont[1]/shell").collapseNode("GVC")
    sap.session.findById("wnd[0]/shellcont[1]/shell").unselectAll()
    sap.session.findById("wnd[0]/shellcont[1]/shell").topNode = "WG_PGK"
    sap.session.findById("wnd[0]/tbar[1]/btn[8]").press()

    sap.session.findById("wnd[0]/usr/cntlGRID1/shellcont/shell").currentCellColumn = "0327"
    sap.session.findById("wnd[0]/usr/cntlGRID1/shellcont/shell").selectedRows = "0"
    sap.session.findById("wnd[0]/usr/cntlGRID1/shellcont/shell").contextMenu()
    sap.session.findById("wnd[0]/usr/cntlGRID1/shellcont/shell").selectContextMenuItem("&XXL")

    sap.session.findById("wnd[1]/tbar[0]/btn[0]").press()
    sap.session.findById("wnd[1]/usr/ctxtDY_PATH").text = str(file_path.parent.absolute())
    sap.session.findById("wnd[1]/usr/ctxtDY_FILENAME").text = str(file_path.name)

    sap.session.findById("wnd[1]/tbar[0]/btn[11]").press()
    sap.session.findById("wnd[0]").close()
    sap.session.findById("wnd[1]/usr/btnSPOP-OPTION1").press()

    sap_exit(file_path)

def sap_contract_zpm(wagons_without_contract, file_path, transaction):

    '''
    Функция по номерам вагонов выполняет поиск номеров договора в zpm_pgk_erv
    
    '''
    
    sap = sap_start(transaction)

    wagons_without_contract.to_clipboard(index=False,header=False)
    sap.session.findById("wnd[0]/usr/ssubGV_SUBSCR_STATPARAM:ZPM_PGK_ERV:1002/btn%_S_WAGNUM_%_APP_%-VALU_PUSH").press()
    sap.session.findById("wnd[1]/tbar[0]/btn[24]").press()
    sap.session.findById("wnd[1]/tbar[0]/btn[8]").press()
    sap.session.findById("wnd[0]/shellcont[1]/shell").expandNode("GVC")
    sap.session.findById("wnd[0]/shellcont[1]/shell").selectItem("114","1")
    sap.session.findById("wnd[0]/shellcont[1]/shell").ensureVisibleHorizontalItem("114","1")
    sap.session.findById("wnd[0]/shellcont[1]/shell").topNode = "GVC"
    sap.session.findById("wnd[0]/shellcont[1]/shell").changeCheckbox("114","1","true")
    sap.session.findById("wnd[0]/shellcont[1]/shell").collapseNode("GVC")
    sap.session.findById("wnd[0]/shellcont[1]/shell").expandNode("RENT")
    sap.session.findById("wnd[0]/shellcont[1]/shell").selectItem("577","1")
    sap.session.findById("wnd[0]/shellcont[1]/shell").ensureVisibleHorizontalItem("577","1")
    sap.session.findById("wnd[0]/shellcont[1]/shell").topNode = "DISLOCATION"
    sap.session.findById("wnd[0]/shellcont[1]/shell").changeCheckbox("577","1","true")
    sap.session.findById("wnd[0]/shellcont[1]/shell").collapseNode("RENT")
    sap.session.findById("wnd[0]/shellcont[1]/shell").unselectAll()
    sap.session.findById("wnd[0]/shellcont[1]/shell").topNode = "WG_PGK"
    sap.session.findById("wnd[0]/tbar[1]/btn[8]").press()
    sap.session.findById("wnd[0]/usr/cntlGRID1/shellcont/shell").selectedRows = "0"
    sap.session.findById("wnd[0]/usr/cntlGRID1/shellcont/shell").contextMenu()
    sap.session.findById("wnd[0]/usr/cntlGRID1/shellcont/shell").selectContextMenuItem("&XXL")

    sap.session.findById("wnd[1]/tbar[0]/btn[0]").press()
    sap.session.findById("wnd[1]/usr/ctxtDY_PATH").text = str(file_path.parent.absolute())
    sap.session.findById("wnd[1]/usr/ctxtDY_FILENAME").text = str(file_path.name)
    sap.session.findById("wnd[1]/tbar[0]/btn[11]").press()
    sap.session.findById("wnd[0]").close()
    sap.session.findById("wnd[1]/usr/btnSPOP-OPTION1").press()

    sap_exit(file_path)

def sap_RM_iw38(toro_orders_for_RM, file_path, transaction):

    '''
    Функция по ТОРО-заказам выполняет поиск рабочих мест в IW38
    
    '''

    sap = sap_start(transaction)

    sap.session.findById("wnd[0]/usr/ctxtDATUV").text = "22.12.2018"
    sap.session.findById("wnd[0]/usr/ctxtDATUV").setFocus
    sap.session.findById("wnd[0]/usr/ctxtDATUV").caretPosition = 10
    toro_orders_for_RM.to_clipboard(index=False,header=False)
    sap.session.findById("wnd[0]/usr/btn%_AUFNR_%_APP_%-VALU_PUSH").press()
    sap.session.findById("wnd[1]/tbar[0]/btn[24]").press()
    sap.session.findById("wnd[1]/tbar[0]/btn[8]").press()
    sap.session.findById("wnd[0]/tbar[1]/btn[8]").press() # ЗАПУСК

    sap.session.findById("wnd[0]/usr/cntlGRID1/shellcont/shell").contextMenu()
    sap.session.findById("wnd[0]/usr/cntlGRID1/shellcont/shell").selectContextMenuItem("&XXL")
    sap.session.findById("wnd[1]/tbar[0]/btn[0]").press()

    sap.session.findById("wnd[1]/usr/ctxtDY_PATH").text = str(file_path.parent.absolute())
    sap.session.findById("wnd[1]/usr/ctxtDY_FILENAME").text = str(file_path.name)
    sap.session.findById("wnd[1]/tbar[0]/btn[11]").press()
    sap.session.findById("wnd[0]").close()
    sap.session.findById("wnd[1]/usr/btnSPOP-OPTION1").press()

    sap_exit(file_path)

def sap_spravka_RM_SQ01(file_path, transaction):

    '''
    Функция выгружает справочник рабочих мест из SQ01
    
    '''

    sap = sap_start(transaction)

    sap.session.findById("wnd[0]/mbar/menu[5]/menu[0]").select()
    sap.session.findById("wnd[1]/usr/radRAD1").select()
    sap.session.findById("wnd[1]/tbar[0]/btn[2]").press()
    sap.session.findById("wnd[0]/tbar[1]/btn[19]").press()
    sap.session.findById("wnd[1]/usr/cntlGRID1/shellcont/shell").currentCellRow = 5
    sap.session.findById("wnd[1]/usr/cntlGRID1/shellcont/shell").selectedRows = "5"
    sap.session.findById("wnd[1]/tbar[0]/btn[0]").press()
    sap.session.findById("wnd[0]/usr/cntlGRID_CONT0050/shellcont/shell").currentCellRow = 11
    sap.session.findById("wnd[0]/usr/cntlGRID_CONT0050/shellcont/shell").selectedRows = "11"
    sap.session.findById("wnd[0]/usr/cntlGRID_CONT0050/shellcont/shell").doubleClickCurrentCell()
    sap.session.findById("wnd[0]/tbar[1]/btn[8]").press()
    sap.session.findById("wnd[0]/usr/txtSP$00001-LOW").text = ""
    sap.session.findById("wnd[0]/usr/ctxtSP$00002-LOW").text = ""
    sap.session.findById("wnd[0]/usr/ctxtSP$00002-LOW").setFocus()
    sap.session.findById("wnd[0]/usr/ctxtSP$00002-LOW").caretPosition = 0
    sap.session.findById("wnd[0]/tbar[1]/btn[8]").press()
    sap.session.findById("wnd[0]/usr/cntlCONTAINER/shellcont/shell").setCurrentCell(11,"KTEXT")
    sap.session.findById("wnd[0]/usr/cntlCONTAINER/shellcont/shell").selectedRows = "11"
    sap.session.findById("wnd[0]/usr/cntlCONTAINER/shellcont/shell").contextMenu()
    sap.session.findById("wnd[0]/usr/cntlCONTAINER/shellcont/shell").selectContextMenuItem("&XXL")

    sap.session.findById("wnd[1]/tbar[0]/btn[0]").press()

    sap.session.findById("wnd[1]/usr/ctxtDY_PATH").text = str(file_path.parent.absolute())
    sap.session.findById("wnd[1]/usr/ctxtDY_FILENAME").text = str(file_path.name)
    sap.session.findById("wnd[1]/tbar[0]/btn[11]").press()
    sap.session.findById("wnd[0]").close()
    sap.session.findById("wnd[1]/usr/btnSPOP-OPTION1").press()

    sap_exit(file_path)