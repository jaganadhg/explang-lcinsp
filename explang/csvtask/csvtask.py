
import os
import pandas as pd

from ..core import (Runnable, SRunnable)


class CSVReadTask(Runnable):
    def __init__(self, file_name):
        self.file_name = file_name
    
    def __verify_path(self):
        if not os.path.exists(self.file_name):
            raise FileNotFoundError(f"File not found: {self.file_name}")

    def run(self, input_data=None):
        """
        Reads data from a CSV file.
        """
        self.__verify_path()
        data = pd.read_csv(self.file_name)
        return data

class CSVWriteTask(Runnable):
    def __init__(self, file_path):
        self.file_path = file_path
    
    def __check_write_permission(self):
        if os.path.exists(self.file_path):
            if not os.access(self.file_path, os.W_OK):
                raise PermissionError(f"No write permission: {self.file_path}")
        else:
            dir_path = os.path.dirname(self.file_path)
            if not os.access(dir_path, os.W_OK):
                raise PermissionError(f"No write permission: {dir_path}")

    def run(self, input_data):
        """
        Writes data to a CSV file.
        """
        if input_data is None or input_data.empty:
            raise ValueError("Input data is empty DataFrame or None.")
        
        self.__check_write_permission()

        input_data.to_csv(self.file_path, index=False)
        return input_data


class SCSVReadTask(SRunnable):
    def __init__(self, file_name):
        self.file_name = file_name
    
    def __verify_path(self):
        if not os.path.exists(self.file_name):
            raise FileNotFoundError(f"File not found: {self.file_name}")

    def run(self, input_data=None):
        """
        Reads data from a CSV file.
        """
        self.__verify_path()
        data = pd.read_csv(self.file_name)
        return data

class SCSVWriteTask(SRunnable):
    def __init__(self, file_path):
        self.file_path = file_path
    
    def __check_write_permission(self):
        if os.path.exists(self.file_path):
            if not os.access(self.file_path, os.W_OK):
                raise PermissionError(f"No write permission: {self.file_path}")
        else:
            dir_path = os.path.dirname(self.file_path)
            if not os.access(dir_path, os.W_OK):
                raise PermissionError(f"No write permission: {dir_path}")

    def run(self, input_data):
        """
        Writes data to a CSV file.
        """
        if input_data is None or input_data.empty:
            raise ValueError("Input data is empty DataFrame or None.")
        
        self.__check_write_permission()

        input_data.to_csv(self.file_path, index=False)
        return input_data