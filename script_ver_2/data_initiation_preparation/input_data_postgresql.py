#!python3

import pandas as pd

if __name__ == '__main__':
    name_list = ['distribution_centers.csv','employees.csv',]
    data = pd.read_csv('data/users.csv')
    print(data.head())
    print(data.shape)
    print(data.columns)