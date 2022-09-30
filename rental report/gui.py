import logging
import queue
import signal
import sys
import threading
import time
import os
import tkinter as tk
from datetime import date
from pathlib import Path
from tkinter import HORIZONTAL, VERTICAL, E, N, S, W, ttk
from tkinter.scrolledtext import ScrolledText
import datetime 
from dateutil.relativedelta import relativedelta

from data_processing import (
    rent_main,
    load_data,
    otchet
)


logger = logging.getLogger("my_log_rent")
start_time = time.perf_counter()
n = 0
button_RM = 0
button_data = 1
button_otchet = 1


# класс для теста показывает логи с ошибками и логи с инфо
class Clock(threading.Thread):
    """Class to display the time every seconds
    Every 5 seconds, the time is displayed using the logging.ERROR level
    to show that different colors are associated to the log levels
    """

    start_time = time.perf_counter()

    def __init__(self):
        super().__init__()
        self._stop_event = threading.Event()

    def run(self):

        logger.debug("Начало работы")

        if n == 0:
            logger.debug("Поставьте необходимые флажки и нажмите перезапустить")
            sys.exit() 

        report_date = datetime.datetime.now()
        #report_date = datetime.datetime.now() + relativedelta(months=-1)

        """ объявляем папку и создаем если ее нет"""
        project_path  = Path(os.getcwd(), "folder_with_uploads_for_rent")
        # project_path = Path(r"C:/Users/ChudinovskikhAO/Desktop/arenda/project") # задаем путь к папке с проектом
        
        spravka = Path(os.getcwd(), 'spravka.xlsx')
        if not spravka.exists():
            logger.debug("Требуется файл spravka.xlsx")
            logger.debug("Положите его в папку откуда запускаете программу и перезапустите")
            sys.exit()

        # try:
        #     spravka = Path(os.getcwd(), 'spravka.xlsx')
        #     # spravka = Path(r"C:/Users/ChudinovskikhAO/Desktop/arenda/project", 'spravka.xlsx')
        # except:
        #     logger.debug("ОШИБКА. Скопируйте в директорию приложения файл spravka.xlsx")
        #     return

        if not project_path.exists():
            logger.debug("Создаем папку для выгрузок отчета аренды")
            project_path.mkdir()
            logger.debug(f"Путь для выгрузок отчета аренды {project_path.absolute()}")

        #try:
        df_zco, df_zco_cut = rent_main(project_path, report_date, spravka, button_RM)

        if button_data == 1:
            load_data(df_zco, project_path)

        if button_otchet == 1:
            otchet(df_zco_cut, spravka, report_date)
        
        # except Exception as err:
        #     print(type(err))
        #     logger.debug(str(err))
        #     sys.exit()

        logger.debug("Завершение работы")
    def stop(self):
        self._stop_event.set()


class QueueHandler(logging.Handler):
    """Class to send logging records to a queue
    It can be used from different threads
    The ConsoleUi class polls this queue to display records in a ScrolledText widget
    """

    def __init__(self, log_queue):
        super().__init__()
        self.log_queue = log_queue

    def emit(self, record):
        self.log_queue.put(record)


class ConsoleUi:
    """Poll messages from a logging queue and display them in a scrolled text widget"""

    def __init__(self, frame):
        self.frame = frame
        # Create a ScrolledText wdiget
        self.scrolled_text = ScrolledText(frame, state="disabled", height=12)
        self.scrolled_text.grid(row=0, column=0, sticky=(N, S, W, E))
        self.scrolled_text.configure(font="TkFixedFont")
        self.scrolled_text.tag_config("INFO", foreground="black")
        self.scrolled_text.tag_config("DEBUG", foreground="black")
        self.scrolled_text.tag_config("WARNING", foreground="orange")
        self.scrolled_text.tag_config("ERROR", foreground="red")
        self.scrolled_text.tag_config("CRITICAL", foreground="red", underline=1)
        # Create a logging handler using a queue
        self.log_queue = queue.Queue()
        self.queue_handler = QueueHandler(self.log_queue)
        formatter = logging.Formatter("%(asctime)s: %(message)s")
        self.queue_handler.setFormatter(formatter)
        logger.addHandler(self.queue_handler)
        # Start polling messages from the queue
        self.frame.after(100, self.poll_log_queue)

    def display(self, record):
        msg = self.queue_handler.format(record)
        self.scrolled_text.configure(state="normal")
        self.scrolled_text.insert(tk.END, msg + "\n", record.levelname)
        self.scrolled_text.configure(state="disabled")
        # Autoscroll to the bottom
        self.scrolled_text.yview(tk.END)

    def poll_log_queue(self):
        # Check every 100ms if there is a new message in the queue to display
        while True:
            try:
                record = self.log_queue.get(block=False)
            except queue.Empty:
                break
            else:
                self.display(record)
        self.frame.after(100, self.poll_log_queue)


def restart_program():
    logger.debug("Версия от 22.07.2022 года")
    logger.debug("Разразработчики: Баринов Д.С., Чудиновских А.О., Жумабаев С.К.")


def main_2():
    global n
    n += 1
    logger.debug(f"Перезапуск {n}")
    clock = Clock()
    clock.start()


class FormUi:
    def __init__(self, frame):
        self.frame = frame

        # Add a button to log the message
        self.button = ttk.Button(self.frame, text="Разработчики", command=restart_program)
        self.button.grid(column=1, row=2, sticky=W)
    

        self.button1 = ttk.Button(self.frame, text="Перезапустить", command=main_2)
        self.button1.grid(column=1, row=3, sticky=W)

    def submit_message(self):
        # Get the logging level numeric valueS
        lvl = getattr(logging, self.level.get())
        logger.log(lvl, self.message.get())


class ThirdUi:
    def __init__(self, frame):
        self.frame = frame
        self.var = tk.BooleanVar(value=0)
        self.check = ttk.Checkbutton(
            self.frame,
            text="Сформировать справочник рабочих мест",
            variable=self.var,
            command=self.on_button
        ).grid(column=1, row=1, sticky=W)

        self.var_upload = tk.BooleanVar(value=1)
        self.check1 = ttk.Checkbutton(
            self.frame,
            text="Сформировать файл с данными",
            variable=self.var_upload,
            command=self.on_button_1,
        ).grid(column=1, row=2, sticky=W)

        self.var_upload_2 = tk.BooleanVar(value=1)
        self.check2 = ttk.Checkbutton(
            self.frame,
            text="Сформировать файл отчета",
            variable=self.var_upload_2,
            command=self.on_button_2,
        ).grid(column=1, row=3, sticky=W)

    def on_button(self):
        global button_RM
        if self.var.get():
            button_RM = 1
            logger.debug("Формирование рабочих мест включено")
        else:
            button_RM = 0
            logger.debug("Формирование рабочих мест выключено")


    def on_button_1(self):
        global button_data
        if self.var_upload.get():
            button_data = 1
            logger.debug("Формирование файла с данными включено")
        else:
            button_data = 0
            logger.debug("Формирование файла с данными выключено")

    def on_button_2(self):
        global button_otchet
        if self.var_upload.get():
            button_otchet = 1
            logger.debug("Формирование отчета включено")
        else:
            button_otchet = 0
            logger.debug("Формирование отчета выключено")


class App:
    def __init__(self, root):
        self.root = root
        root.title("Отчет аренды")
        root.columnconfigure(0, weight=1)
        root.rowconfigure(0, weight=1)
        # Create the panes and frames
        style = ttk.Style()
        style.configure("BW.TLabel", background="firebrick4")
        vertical_pane = ttk.PanedWindow(self.root, orient=VERTICAL)
        vertical_pane.grid(row=0, column=0, sticky="nsew")
        horizontal_pane = ttk.PanedWindow(vertical_pane, orient=HORIZONTAL)
        vertical_pane.add(horizontal_pane)
        form_frame = ttk.Labelframe(horizontal_pane, text="Нажмите кнопку")
        form_frame.columnconfigure(1, weight=1)
        horizontal_pane.add(form_frame, weight=1)
        console_frame = ttk.Labelframe(horizontal_pane, text="Консоль")
        console_frame.columnconfigure(0, weight=1)
        console_frame.rowconfigure(0, weight=1)
        horizontal_pane.add(console_frame, weight=1)
        third_frame = ttk.Labelframe(vertical_pane, text="Измените, если требуется:")
        vertical_pane.add(third_frame, weight=5)
        # Initialize all frames
        self.form = FormUi(form_frame)
        self.console = ConsoleUi(console_frame)
        self.third = ThirdUi(third_frame)
        self.clock = Clock()
        self.clock.start()
        self.root.protocol("WM_DELETE_WINDOW", self.quit)
        self.root.bind("<Control-q>", self.quit)
        signal.signal(signal.SIGINT, self.quit)

    def quit(self, *args):
        self.clock.stop()
        self.root.destroy()


def main():
    logging.basicConfig(level=logging.DEBUG)
    root = tk.Tk()
    app = App(root)
    app.root.mainloop()


if __name__ == "__main__":
    main()