#!/usrbin/env python

import os 
import pandas as pd
import pytest 

from explang.csvtask import (CSVWriteTask, CSVReadTask)


class CSVLseTest:
    @pytest.fixture
    def sample_data(self):
        return pd.DataFrame({
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35]
        })
    
    @pytest.fixture
    def temp_file(self,tmp_path):
        return tmp_path / 'test.csv'
    
    def test_csv_read(self, sample_data, temp_file):
        sample_data.to_csv(temp_file, index=False)
        task = CSVReadTask(temp_file)
        data = task.run()
        assert data.equals(sample_data)
    
    def test_csv_write(self, sample_data, temp_file):
        task = CSVWriteTask(temp_file)
        task.run(sample_data)
        data = pd.read_csv(temp_file)
        assert data.equals(sample_data)
    
    def test_csv_pipeline(self, sample_data, temp_file):
        task = CSVWriteTask(temp_file) | CSVReadTask(temp_file)
        data = task.run(sample_data)
        assert data.equals(sample_data)