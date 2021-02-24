import os

import pandas as pd


PATH_FILE_IN = '<your file name>'
SEPARATOR = ','
PATH_DIRECTORY_OUT = 'out'  # only directory without '/' in the end
ENCODING = 'utf8'
CHUNKSIZE = 50000
SAVE_INDEX = False  # True - save index column in output file
BATCH = 0


def show_list_column(data):
    """Show for user all columns with index of DataFrame
    and return list of columns.
    """
    for gm_chunk in data:
        df_coumns_names = list(gm_chunk.columns)
        df_coumns_names_with_index = [(i, df_coumns_names.index(i)) for i in df_coumns_names]
        print('\nAll columns in df: \n')
        for i, k in df_coumns_names_with_index:
            print(i, '-', k)
        break
    return df_coumns_names


def create_out_folder():
    if not os.path.exists(f'{PATH_DIRECTORY_OUT}'):
        os.makedirs(f'{PATH_DIRECTORY_OUT}')


if __name__ == '__main__':
    create_out_folder()

    chunksize = input(f'\nEnter number of chunksize for DataFrame[{CHUNKSIZE}]: ')
    if chunksize == '':
        chunksize = CHUNKSIZE
    else:
        try:
            chunksize = int(chunksize)
        except ValueError:
            print('ValueError, enter int type')
            exit()

    separator = input(f'\nEnter separator of your DataFrame[{SEPARATOR}]: ')
    if separator == '':
        separator = SEPARATOR

    data = pd.read_csv(PATH_FILE_IN,
                       encoding=ENCODING,
                       chunksize=chunksize,
                       sep=separator)

    df_coumns_names = show_list_column(data)
    search_column = int(input('\nEnter index of the column you want to search: '))
    search_word = input('\nText for search: ')

    for gm_chunk in data:
        # print(gm_chunk)
        new_df = 0
        new_df = gm_chunk.loc[gm_chunk[df_coumns_names[search_column]].str.contains(f'{search_word}', na=False)]
        if not new_df.empty:
            new_df.to_csv(f'out/{search_word}.csv',
                          mode='a',
                          sep=',',
                          header=(not os.path.exists(f'{PATH_DIRECTORY_OUT}/{search_word}.csv')),
                          index=SAVE_INDEX)
        BATCH += chunksize
        print(f'{BATCH} records processed')

    print('All done')
