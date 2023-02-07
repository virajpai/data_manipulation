import numpy as np
import pandas as pd

from datetime import datetime

class Utils:
    @staticmethod
    def tprint(txt):
        print('{} :: {}'.format(str(datetime.now()), txt))       

class ProcessData:
    
    def __init__(self, filepath):
        
        # Read Data
        Utils.tprint('Reading File Path')
        self.df = pd.read_csv(filepath, skiprows=3, low_memory=False)
        
        # Dropping All Null Columns
        Utils.tprint('Dropping All Null Columns')
        all_null_cols = [col for col in self.df.columns if self.df[col].isnull().all()]
        self.df.drop(columns=all_null_cols, inplace=True)
        
        # Vectorizing get_all_dtypes
        self.get_all_types = np.vectorize(self.get_all_types)
    
    def get_all_types(self, type_tuple):
        _segment = ''
        _period = ''
        _type = ''
        _subtype = ''
        _comment = ''

        try:
            _tmp = type_tuple[0].split(' - ')
            _segment = _tmp[0]
            _period = _tmp[1]
            _type = type_tuple[1]
            _subtype = type_tuple[2]
            _comment = "processed sucessfully"

        except Exception as e:
            _comment = "processing failed: {}".format(e)

        return _segment, _period, _type, _subtype, _comment, str(datetime.now())

    
    def build_base_data(self):
        
        
        Utils.tprint('Started Building Base Output')
        
        # Transpose the dataframe
        Utils.tprint('Transposing Data')
        df_transpose = self.df.T.reset_index(drop=True)
        
        # Set column headders to row 1 by replacing 1st 3 values accordingly
        Utils.tprint('Cleaning Column Headers')
        headers = df_transpose.iloc[0]
        
        headers.at[0] = 'Segment - Period'
        headers.at[1] = 'Type'
        headers.at[2] = 'Subtype'
        
        df_transpose.columns = headers
        
        df_transpose.drop(index=0, inplace=True)
        df_transpose.reset_index(drop=True, inplace=True)
        
        # Fill Type with Forward Fill Strategy impute nulls with last non-missing value
        Utils.tprint('Imputing Type by last non-missing value')
        df_transpose['Type'].fillna(method='ffill', inplace=True)
        
        # Convert All types to index leaving only date columns for unpivoting
        Utils.tprint('Unpivoting the data')
        cols_to_idx = ['Segment - Period', 'Type', 'Subtype']
        df_transpose.index = df_transpose[cols_to_idx]
        df_transpose.drop(columns=cols_to_idx, inplace=True)
        
        # unpivot the data
        df_T_unstacked = df_transpose.unstack()
        df_T_unstacked = pd.DataFrame(df_T_unstacked)
        df_T_unstacked.columns = ['Value']
        df_T_unstacked = df_T_unstacked.reset_index()
        Utils.tprint('Data to process: {}'.format(df_T_unstacked.shape))
        
        # Utils.tprint(df_T_unstacked['level_1'].head())
        
        # Breaking all the types to required columns 
        Utils.tprint('Breaking all the types to required columns')
        all_types = self.get_all_types(df_T_unstacked['level_1'])
        Utils.tprint('Type Breakdown created')
        type_cols = ['Segment', 'Period', 'Type', 'Subtype', 'Comment', 'Processed Datetime']
        
        for n, col in enumerate(type_cols):
            Utils.tprint('{} {}'.format(n, col))
            df_T_unstacked[col] = all_types[n]
        
        # Utils.tprint(df_T_unstacked.head())
        
        # Process date and value
        Utils.tprint('Formatting Date & Value Column')
        df_T_unstacked.rename(columns={0: 'Date'}, inplace=True)
        df_T_unstacked['Date'] = pd.to_datetime(df_T_unstacked['Date'])
        df_T_unstacked['Value'] = df_T_unstacked['Value'].astype(float)
        
        # Finalizing Base Data
        req_cols = ['Date', 'Segment', 'Period', 'Type', 'Subtype', 'Value']
        self.base_output_df = df_T_unstacked[req_cols].copy()
        
        df_T_unstacked.to_csv('../data/interim/base_output.csv', index=False)
        Utils.tprint('Base Data Created and Saved!')
    
    def summerize_data(self):
        
        Utils.tprint('Started summerizing data')
        base_output_df = self.base_output_df.copy()
        
        # print(base_output_df.dtypes)
        
        ### 1 - Sum of Billings by country
        Utils.tprint('Calculating Sum of Billings by Country...')
        billing_by_ctry = base_output_df[base_output_df['Type'] == 'Countries'].groupby('Subtype')['Value'].sum().reset_index(drop=False)
        billing_by_ctry.columns = ['Countries', 'Billings']
        
        ### 2 - Sum of billing by period for type = market from 2016 to present (include Jan, 1, 2016)
        Utils.tprint("Calculating Sum of billing by period for type = market from 2016 to present...")
        billing_by_period = base_output_df[
            (base_output_df['Type'] == 'Market')
            & (base_output_df['Date'].dt.year >= 2016)
        ].groupby('Period')['Value'].sum().reset_index(drop=False)
        billing_by_period.columns = ['Period', 'Billings']
        
        ### 3 - Summary Statistics by Segment
        Utils.tprint("Calculating Summary Stats by Segment...")
        segment_summary_stat = base_output_df.groupby('Segment').describe()
        
        ### Writing to Final Output
        Utils.tprint('Writing final summerized output')
        
        try:
            xw = pd.ExcelWriter('../data/processed/output.xlsx')
            billing_by_ctry.to_excel(xw, sheet_name='Output', index=False, startrow=3)
            billing_by_period.to_excel(xw, sheet_name='Output', index=False, startcol=4, startrow=3)
            segment_summary_stat.to_excel(xw, sheet_name='Segment Summary Stats')
        finally:
            xw.close()
            
        Utils.tprint('Processing complete!')
        
        
    
if __name__ == '__main__':
    filepath = '../data/raw/Python Test 1 - billings_europe.csv'
    prd = ProcessData(filepath)
    prd.build_base_data()
    prd.summerize_data()