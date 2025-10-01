import os
import zipfile
from lxml import etree
import pandas as pd
import tkinter as tk
from tkinter import filedialog

def analyze_xml_in_zip(archive, fname, file_path):
    try:
        with archive.open(fname) as fileobj:
            for event, elem in etree.iterparse(fileobj, events=("start",)):
                nsmap = dict(elem.nsmap)
                root_tag = elem.tag
                info = {
                    "zipfile": os.path.basename(file_path),
                    "xml_filename": fname,
                    "root_tag": root_tag
                }
                for k, v in nsmap.items():
                    info[f'ns_{k if k else "default"}'] = v
                return info
    except Exception as e:
        return {
            "zipfile": os.path.basename(file_path),
            "xml_filename": fname,
            "root_tag": None,
            "error": str(e)
        }

def collect_ns_info_zip(file_path):
    records = []
    with zipfile.ZipFile(file_path) as archive:
        for fname in archive.namelist():
            if fname.endswith(".fb2") and not fname.endswith("/"):
                info = analyze_xml_in_zip(archive, fname, file_path)
                records.append(info)
    return pd.DataFrame(records)

def collect_all_ns_zip(file_name):
    folder = os.path.dirname(file_name)
    zip_files = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".zip") and os.path.isfile(os.path.join(folder, f))]
    all_result = pd.DataFrame()
    for zip_path in zip_files:
        df = collect_ns_info_zip(zip_path)
        all_result = pd.concat([all_result, df], ignore_index=True)
    return all_result

def select_zip_file(initialdir='./lib'):
    """Открывает диалог выбора zip-файла и возвращает путь к выбранному файлу или None."""
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

# Пример вызова:
file_path = select_zip_file()
df = collect_all_ns_zip(file_path)