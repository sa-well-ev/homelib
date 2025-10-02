import tkinter as tk
from tkinter import filedialog
from files_processing import process_zipfolder
from xml_processor import namespaces

def select_zip_file(initialdir='./lib'):
    """
    Открывает диалог выбора zip-файла и возвращает путь к выбранному файлу или None.

    Args:
        initialdir (str, optinal): Директория в котором открывается окно (default='./lib')        

    Returns:
        str: Полный пусть к выбранному файлу и None если файл не выбран.
    """
    root = tk.Tk()
    root.withdraw()
    file_path = filedialog.askopenfilename(
        initialdir=initialdir,
        title="Выберите файл архива в рабочей папке",
        filetypes=[("Zip files", "*.zip"), ("All files", "*.*")]
    )
    if file_path:
        print(f"Выбран файл: {file_path}")
    else:
        print("Файл не выбран.")
    root.destroy()
    return file_path

if __name__ == "__main__":
    file_path = select_zip_file()
    if file_path:
        df = process_zipfolder(file_path, namespaces)        
