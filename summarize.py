import argparse
import csv
import sys
import yfinance as yf
from decimal import *


def lookup_col_index(headers):
    indexes = {}
    for i, h in enumerate(headers):
        indexes[h] = i
    return indexes


def summarize(input_file, output_file):
    with open(input_file, newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='|')
        col_index = lookup_col_index(reader.__next__())
        summary = {}
        for row in reader:
            if 'ticker' not in col_index:
                print(col_index)
            ticker = row[col_index['ticker']]
            tx_type = row[col_index['tx_type']]
            units = row[col_index['units']]
            if ticker == "":
                continue
            if ticker not in summary:
                summary[ticker] = {
                    'balance': Decimal(0),
                    'units': Decimal(0)
                }
            summary[ticker]['balance'] += Decimal(row[col_index['amount']])
            if tx_type in ['BUY', 'SELL']:
                summary[ticker]['units'] += Decimal(units)
        for ticker, v in summary.items():
            if v['units'] > 0:
                stock = yf.Ticker(ticker)
                price = round(stock.info['regularMarketPrice'] if 'regularMarketPrice' in stock.info else (stock.info['bid'] + stock.info['ask']) / 2, 3)
                v['balance'] += v['units'] * Decimal(price)
        for ticker, v in sorted(summary.items()):
            print(f'{ticker},{v["balance"]}')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('input_file', help='Path to the CSV containing transactions to summarize.')
    args = parser.parse_args()
    csv_file = args.input_file
    summarize(csv_file, sys.stdout)


if __name__ == '__main__':
    main()