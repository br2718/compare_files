import pandas as pd
import numpy as np
import sys
import json
import os
import os.path
from collections import namedtuple, Counter
import traceback
import csv
import re
import itertools

def read_json_file(jsonfile):
    if(not os.path.isfile(jsonfile)):
        raise FileNotFoundError('This file does not exist or this is not a file: {path}.'.format(path=jsonfile))   
    
    with open(jsonfile) as j:
        config = json.load(j)
        
    Path = namedtuple("Path", ["pathname", "hasHeader", "includeHeader", "columnsToCompare", "columnsToSortBy"])    
    lstPaths = [Path(p["name"], p["hasHeader"], p["includeHeader"], p["columnsToCompare"], p["columnsToSortBy"]) for p in config["paths"]]
    dir_out = config["outputDirectory"]
    return tuple([lstPaths, dir_out])
    
def isFile(ntupPath):
    try:
        if(not os.path.isfile(ntupPath.pathname)):
            raise FileNotFoundError('not a file: {}.'.format(ntupPath.pathname))
    except FileNotFoundError as e:
        traceback.print_exc()
        sys.exit(1)

def check_columns_to_compare(ntupPath):
    try:
        if(len(ntupPath.columnsToCompare) <= 0):
            raise ValueError('no column numbers given so no comparison can be performed on file {}'.format(ntupPath.pathname))
    except ValueError as e:
        traceback.print_exc()
        sys.exit(1)
        
def check_input_paths(lstPaths, minMaxInputPathCounts, *func):    
    try:
        numPaths = len(lstPaths)
        if(numPaths < minMaxInputPathCounts[0]):        
            raise TypeError('{0} input paths less than {1}, the minimum allowed.'.format(numPaths, minMaxInputPathCounts[0])) 

        if(numPaths > minMaxInputPathCounts[1]):
            raise TypeError('{0} input paths greater than {1}, the maximum allowed.'.format(numPaths, minMaxInputPathCounts[1]))        
        
        # Perform other checks by running other functiona
        [f(p) for p in lstPaths for f in func]
        
    except TypeError as e:
        traceback.print_exc()
        sys.exit(1)
        
def isDirectory(dir):
    return os.path.isdir(dir)
    
def create_output_dir(dir):
    try:
        new_dir = os.path.abspath(os.path.join(os.getcwd(), dir))
        os.mkdir(new_dir)           
        return new_dir
    except FileExistsError as e:
        return new_dir
        
def make_path(*pathparts):
    return ''.join(*pathparts)
    
def get_filename_and_extension(filename):
    _, fname = os.path.split(filename)
    return os.path.splitext(fname)
    
def write_compared_values_to_file(lstDf, pathprefix = '', path='temp.csv', pathsuffix='', dir_out=''):
    try:       
        if(dir_out[-1] != os.sep): dir_out = dir_out + os.sep
    except IndexError as e:
        pass   
    
    lstPathOut = list()
    lstDfAndSuffix = [([lstDf[0], "_inmainfileonly"]), ([lstDf[1], "_insecondfileonly"]), ([lstDf[2], "_inbothfiles"])]
    for df, psuffix in lstDfAndSuffix:        
        fname_no_ext, ext = get_filename_and_extension(path)
        path_out = make_path([dir_out, pathprefix, fname_no_ext, psuffix, ext])
        lstPathOut.append(path_out)
        df.to_csv(path_out, index=False, quoting=csv.QUOTE_NONE) 
        
    return lstPathOut
    
def compare_files_by_column(dfMain, df):
    df.columns = dfMain.columns
    dfMergedInMainFileOnly = pd.merge(dfMain, df, how='left', indicator=True).query("_merge == 'left_only'").drop(['_merge'], axis=1)
    dfMergedInSecondFileOnly = pd.merge(dfMain, df, how='right', indicator=True).query('_merge == "right_only"').drop(['_merge'], axis=1)
    dfMergedInBoth = pd.merge(dfMain, df, how='inner', indicator=True).query('_merge == "both"').drop(['_merge'], axis=1)
    return [dfMergedInMainFileOnly, dfMergedInSecondFileOnly, dfMergedInBoth]
    
def create_df(path, usecols=None,sortcols=None):
    textFileReader = pd.read_csv(path, usecols=usecols, dtype='str', chunksize = 10000, iterator=True)
    df = pd.concat(textFileReader, ignore_index=True)
    df.fillna('', inplace=True)
    if(sortcols is not None):
        lstCols = [df.columns[x] for x in sortcols]
        df.sort_values(by=lstCols, axis=0, ascending=[True]*len(lstCols), inplace=True, na_position='first',kind='mergesort')
    return df
    
def compare_files(lstPaths, dir_out):
    lstDfs = [create_df(ntupPath.pathname, ntupPath.columnsToCompare, ntupPath.columnsToSortBy) for ntupPath in lstPaths]
    lstDfsComparisons = [compare_files_by_column(lstDfs[0], df) for df in lstDfs[1:]]
    lstPathsOut = [write_compared_values_to_file(lst, 'out_', ntupPath.pathname, '', dir_out) for lst in lstDfsComparisons for ntupPath in lstPaths[1:]]
    lstPathsOut.insert(0, ['','',''])
    return zip(lstPaths, lstDfs, lstPathsOut)
    
def print_line_counts_from_file(lstPaths, lineBeforeText=None, lineAfterText=None, total=False):
    if(lineBeforeText is not None): 
        print(lineBeforeText, end='\n', file=sys.stderr)
    
    tot = 0  
    for p in lstPaths:
        lngth = len(create_df(ntupPath.pathname).index)             
        print('{}     {}'.format(p.pathname, lngth, sep='\t', end='\n', file=sys.stderr))
        if(total):
            tot = tot + lngth
              
    if(lineAfterText is not None):
        print(lineAfterText, file=sys.stderr)   
        
    if(total): 
        print(tot, file=sys.stderr)
        
def field_diff(main, otro):
    if(x == y):
        return '' 
    else:
        return '{} ---> {}'.format(x, y)
        
def check_leading_spaces(lstTupRow, lstTupCols):
    reasons = ''
    pattern = r"^\s.+" #white space at beginning of word
    
    for tuprow, tupcols in zip(lstTupRow, lstTupCols):     
        col1, col2 = tuprow
        colname1, colname2 = tupcols
        if(re.search(pattern, col1) and col2 == col1.rstrip()):            
            reasons = ('{}: {} -- {}'.format(col1, 'has leading whitespace)'))
        elif(re.search(pattern, col2) and col1 == col2.rstrip()):            
            reasons = ('{}: {} -- {}'.format(col2, 'has leading whitespace)'))
    
    return reasons
    
def check_trailing_spaces(lstTupRow, lstTupCols):
    reasons = ''
    pattern = r".+\s$" #white space at end of word
    
    for tuprow, tupcols in zip(lstTupRow, lstTupCols):  
        col1, col2 = tuprow
        colname1, colname2 = tupcols
        if(re.search(pattern, col1) and col2 == col1.rstrip()):            
            reasons = ('{}: {} -- {}'.format(colname1, col1, 'has trailing whitespace)'))
        elif(re.search(pattern, col2) and col1 == col2.rstrip()):            
            reasons = ('{}: {} -- {}'.format(colname2, col2, 'has trailing whitespace)'))
    
    return reasons
    
def check_for_mismatches(tupRow, tupColNames, lstReasons):
    for tupData, tupColNames, lstReas in zip(tupRow, tupColNames, lstReasons):         
        for i in list(range(0, len(tupData), 2)):
            if(all(list(filter(lambda x: x!='', lstReas))) and tupData[i] != tupData[i+1]): 
                lstReas.append('mismatch on {}: |{}| <> |{}|'.format(tupColNames[i][:-5], tupData[i], tupData[i+1]))   
            else:
                lstReas.append('')
                
    return lstReasons
    
def check_fields(row, funcs, lstTupColsToCheck):    
    lstTupRow = [(row[x], row[y]) for (x,y) in lstTupColsToCheck]
    res = [[f([tup], [cols]) for f in funcs] for tup, cols in zip(lstTupRow, lstTupColsToCheck)] 
    lstReasons = check_for_mismatches(lstTupRow, lstTupColsToCheck, res)
    return lstReasons
    
def write_reasons_for_unmatching_fields_to_file(lstPaths, lstDfs, lstPathsOut):       
    lstSearchCols = [list(lstDfs[0].columns)[x] for x in lstPaths[0].columnsToSortBy]
    
    for p, df, pathsOut in zip(lstPaths[1:], lstDfs[1:], lstPathsOut[1:]):   
        df_inmainonly = create_df(pathsOut[0])  
        df_commonids = pd.merge(df_inmainonly, df, on=lstSearchCols, how='inner', suffixes=["_main", "_otro"],copy=False)
        lstTupColsToCheck = [(col+"_main", col+"_otro") for col in df.columns if col not in lstSearchCols]
        lstCheckCols = list()
        for tup in lstTupColsToCheck:
            lstCheckCols.extend([*tup])

        df_check = df_commonids.loc[:, (lstCheckCols)]       
        df_check['addl_reasons'] = ''
        funcs = [check_leading_spaces, check_trailing_spaces]
        lstReasons = df_check.apply(check_fields, axis=1, args=(funcs,lstTupColsToCheck))
        df_check['addl_reasons'] = ' '.join([val for lst in lstReasons for val in lst])
        df_check.to_csv('unmatching_reasons.csv')
        
def main():         
    jsonfile = "config.json" # sys.argv[1]  
    minMaxInputPathCounts = tuple([2,2])

    lstPaths, dir_out = read_json_file(jsonfile)
    check_input_paths(lstPaths, minMaxInputPathCounts, isFile, check_columns_to_compare)          
    dir_out = create_output_dir(dir_out)             
    lstPaths, lstDfs, lstPathsOut = zip(*compare_files(lstPaths, dir_out))
    write_reasons_for_unmatching_fields_to_file(lstPaths, lstDfs, lstPathsOut)
    
if(__name__ == '__main__'):
    main()