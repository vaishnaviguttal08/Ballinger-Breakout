# Import necessary modules
import pandas as pd
import sys
import warnings
import time
import os
import pyodbc
from datetime import datetime
from collections import defaultdict
warnings.simplefilter(action='ignore', category=FutureWarning)

# Define customizable inputs for backtesting
def inputs():
    
    server_name = 'DESKTOP-F4BFIT2'  # SQL Server Name
    database_name = 'ACCORD_DATA'   
    table_name = 'ACCORD_DATA'   
    local_folder_path =  "C:\\Users\\A1\\vaishnavig\\UpworkBollinger\\backtest_results"
    execute_entry_exit = "on-close"  
    minimum_price = 1               

    max_holdings_count = 25         
    sharpe_period = 180             
    initial_capital = 2500000       
    min_mcap = 1               
    max_mcap = 750                 
   
 
    return server_name, database_name, table_name, execute_entry_exit, minimum_price, max_holdings_count, sharpe_period, initial_capital, local_folder_path, min_mcap, max_mcap


# Fetch data for each ticker from the database
def get_data(server_name, database_name, table_name, ticker_i):
    try:
        connection_string = f'DRIVER={{SQL Server}};SERVER={server_name};DATABASE={database_name};Trusted_Connection=yes;'
        with pyodbc.connect(connection_string) as conn:
            with conn.cursor() as cursor:
                #Adjut date:

                 query =   f"""
                                    SELECT A_Date, A_Close, RANK_PMA_MCAP, Sharpe_30, Sharpe_90, Sharpe_180, Sharpe_365, BB_U_100D_3SD, BB_D_100D_1SD 
                                    FROM dbo.{table_name} 
                                    WHERE NSE_Symbol = '{ticker_i}' 
                                    AND YEAR(A_Date) = 2011
                                    AND BB_U_100D_3SD IS NOT NULL 
                                    AND BB_D_100D_1SD IS NOT NULL
                                    ORDER BY A_Date
                                    """
                 
    except pyodbc.Error as e:
        
        print(f"Error: {e}")
        return []

    
print("Database connection successful")

def ticker_backtest(data, ticker_, execute_entry_exit, minimum_price):
    entry_exit = []
    in_a_trade = 0

    for i, row in enumerate(data):
        if i+1 < len(data) and row[1] is not None and row[1] >= minimum_price:  # Ensure minimum price condition
            current_close = row[1]

            if execute_entry_exit == "on-close":
                execution_date = data[i+1][0]
                execution_price = data[i+1][1]

            upper_band = row[7]
            lower_band = row[8]

            # Check if upper_band and lower_band are not None before comparing
            if in_a_trade == 0 and upper_band is not None and current_close > upper_band:  # Entry signal (close above upper band)
                in_a_trade = 1

                entry_mcap_rank = round(row[2], 2) if row[2] is not None else None
                entry_sharpe_30 = round(row[3], 2) if row[3] is not None else None
                entry_sharpe_90 = round(row[4], 2) if row[4] is not None else None
                entry_sharpe_180 = round(row[5], 2) if row[5] is not None else None
                entry_sharpe_365 = round(row[6], 2) if row[6] is not None else None

                entry_date = execution_date
                entry_price = round(execution_price, 2)

            elif in_a_trade == 1 and lower_band is not None and current_close < lower_band:  # Exit signal (close below lower band)
                exit_date = execution_date
                exit_price = round(execution_price, 2)

                pnl = round((exit_price - entry_price) / entry_price * 100, 2)
                holding_period = (datetime.strptime(exit_date, "%Y-%m-%d") - datetime.strptime(entry_date, "%Y-%m-%d")).days

                entry_exit.append([
                    ticker_, entry_date, entry_price, exit_date, exit_price,
                    "Bollinger Exit", holding_period, pnl, entry_mcap_rank,
                    entry_sharpe_30, entry_sharpe_90, entry_sharpe_180, entry_sharpe_365
                ])

                in_a_trade = 0  # Reset for the next trade

    return entry_exit  # Return trade results for the ticker


# Main function to process all tickers
def all_tickers_backtest():
    server_name, database_name, table_name, execute_entry_exit, minimum_price, max_holdings_count, sharpe_period, initial_capital, local_folder_path, min_mcap, max_mcap = inputs()

    connection_string = f'DRIVER={{SQL Server}};SERVER={server_name};DATABASE={database_name};Trusted_Connection=yes;'

    try:
        with pyodbc.connect(connection_string) as conn:
            with conn.cursor() as cursor:
                query = f"SELECT DISTINCT(NSE_Symbol) FROM dbo.{table_name} ORDER BY NSE_Symbol"
                cursor.execute(query)
                unique_tickers = cursor.fetchall()
    except pyodbc.Error as e:
        print(f"Error: {e}")

    combined_df_entry_exit = pd.DataFrame()
    for i, ticker_ in enumerate(unique_tickers):
        data = get_data(server_name, database_name, table_name, ticker_[0])

        if data:
            entry_exit = ticker_backtest(data, ticker_[0], execute_entry_exit, minimum_price)

            columns = ["Ticker", "Entry Date", "Entry Price", "Exit Date", "Exit Price", "Exit Type", "Holding (days)", "PnL", "Mcap Rank", "Sharpe 30", "Sharpe 90", "Sharpe 180", "Sharpe 365"]
            if len(entry_exit) > 0:
                df_entry_exit = pd.DataFrame(entry_exit, columns=columns)
                combined_df_entry_exit = pd.concat([combined_df_entry_exit, df_entry_exit], ignore_index=True)

    last_5_columns = combined_df_entry_exit.iloc[:, -5:]
    df_filtered = combined_df_entry_exit.drop(last_5_columns[last_5_columns.isna().any(axis=1)].index)
    return df_filtered, sharpe_period, initial_capital, max_holdings_count, local_folder_path, min_mcap, max_mcap



# Function to save the portfolio to a single CSV file
def save_portfolio_to_csv(portfolio_df, file_path):
    
    # Ensure "Entry Date" column is in datetime format
    portfolio_df['Entry Date'] = pd.to_datetime(portfolio_df['Entry Date'])
    # Sort by "Entry Date"
    portfolio_df = portfolio_df.sort_values(by='Entry Date')
    # Save to CSV
    portfolio_df.to_csv(file_path, index=False)
    print(f"Portfolio saved in date order to {file_path}")

    
def portfolio():
    # Fetch the backtest data
    df_filtered, sharpe_period, initial_capital, max_holdings_count, local_folder_path, min_mcap, max_mcap = all_tickers_backtest()

    # Prepare data for processing
    list_all_trades = df_filtered.values.tolist()
    unsorted_dates = df_filtered['Entry Date'].unique().tolist()
    sorted_dates_datetime = sorted([datetime.strptime(date, '%Y-%m-%d') for date in unsorted_dates])
    unique_entry_dates = [date.strftime('%Y-%m-%d') for date in sorted_dates_datetime]

    # Organize trades by entry date
    result_dict = defaultdict(list)
    for row in list_all_trades:
        result_dict[row[1]].append(row)
    dict_all_trades = dict(result_dict)

    # Mapping for Sharpe period selection
    sharpe_dict = {30: 9, 90: 10, 180: 11, 365: 12}
    sharpe_selection = sharpe_dict[sharpe_period]

    active_holdings = []
    total_holdings = []
    current_capital = initial_capital

    # Iterate over unique entry dates to process trades
    for date in unique_entry_dates[:-1]:
        # Check for exit conditions and free up capital
        if active_holdings:
            for holding in active_holdings[:]:
                if holding[3] != "":
                    current_date = datetime.strptime(date, '%Y-%m-%d')
                    holding_exit_date = datetime.strptime(holding[3], '%Y-%m-%d')
                    if holding_exit_date <= current_date:
                        active_holdings.remove(holding)
                        current_capital += round((int(holding[14]) * float(holding[4])), 0)
                        

        # Calculate available spots and filter signals based on Mcap
        active_holdings_count = len(active_holdings)
        spots_left = max_holdings_count - active_holdings_count
        signals_in_date = len(dict_all_trades[date])


        if spots_left != 0:
            # Filter trades by market capitalization (min_mcap and max_mcap)
            filtered_by_mcap = [signal for signal in dict_all_trades[date] if min_mcap <= signal[8] <= max_mcap]
            

            if len(filtered_by_mcap) <= spots_left:
                # Allocate capital equally among all trades
                capital_per_holding = current_capital // spots_left
                current_capital -= capital_per_holding * len(filtered_by_mcap)
                

                # Add trades to active holdings
                for iter in filtered_by_mcap:
                    iter.append(round(capital_per_holding, 2))
                    iter.append(round(capital_per_holding // iter[2], 0))  # Number of shares to buy
                    iter.append((iter[4] - iter[2]) * iter[-1])  # Potential PnL if sold at exit price
                active_holdings.extend(filtered_by_mcap)
                total_holdings.extend(filtered_by_mcap)

            else:
                # Sort trades by Sharpe ratio and select top ones
                sorted_by_sharpe = sorted(filtered_by_mcap, key=lambda x: float(x[sharpe_selection]), reverse=True)[:spots_left]
                

                # Calculate the capital allocation for selected trades
                capital_per_holding = current_capital // spots_left
                current_capital -= capital_per_holding * len(sorted_by_sharpe)
                

                # Add the top-ranked trades to active holdings
                for iter in sorted_by_sharpe:
                    iter.append(round(capital_per_holding, 2))
                    iter.append(round(capital_per_holding // iter[2], 0))  # Number of shares to buy
                    iter.append((iter[4] - iter[2]) * iter[-1])  # Potential PnL if sold at exit price
                active_holdings.extend(sorted_by_sharpe)
                total_holdings.extend(sorted_by_sharpe)
                

    # Create DataFrame for the portfolio
    portfolio_df = pd.DataFrame(total_holdings, columns=[ 
        'Ticker', 'Entry Date', 'Entry Price', 'Exit Date', 'Exit Price', 
        'Exit Type', 'Holding (days)', 'PnL', 'Mcap Rank', 'Sharpe 30', 'Sharpe 90',
        'Sharpe 180', 'Sharpe 365', 'Allocated Capital', 'Shares', 'Potential PnL' 
    ])

    return portfolio_df, current_capital



    def save_portfolio_to_csv(portfolio_df, file_path):
        if os.path.exists(file_path):
       
            portfolio_df.to_csv(file_path, mode='a', index=False, header=False)
                
        else:
       
            portfolio_df.to_csv(file_path, index=False)
    

# Execution logic
if __name__ == "__main__":

    
    # File path for saving portfolio data
    file_path = 'C:\\Users\\A1\\vaishnavig\\UpworkBollinger\\backtest_results\\portfolio_summary.csv'


    # Get final portfolio and remaining capital
    portfolio_df, remaining_capital = portfolio()



    # Save the portfolio to a single CSV file
    save_portfolio_to_csv(portfolio_df, file_path)



    # Print final portfolio summary and remaining capital
    print(f"\nFinal Portfolio Summary: \n{portfolio_df}")
    print(f"\nRemaining Capital after all trades: {remaining_capital}")
