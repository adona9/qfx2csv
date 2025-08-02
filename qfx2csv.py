import json
import os
import requests_cache
import traceback

import pandas as pd
import pytz
from decimal import Decimal

import yfinance as yf
from ofxtools import OFXTree
from ofxtools.models.invest.transactions import BUYOPT
from ofxtools.models.invest.transactions import BUYSTOCK
from ofxtools.models.invest.transactions import CLOSUREOPT
from ofxtools.models.invest.transactions import SELLOPT
from ofxtools.models.invest.transactions import SELLSTOCK
from ofxtools.models.invest.transactions import INCOME
from ofxtools.models.invest.transactions import INVBANKTRAN

import csv
import re
import argparse
from datetime import datetime, timezone, timedelta

# https://ofxtools.readthedocs.io/en/latest/parser.html


unknown_security = {
    'ticker': 'unknown',
    'name': 'unknown'
}


def get_ticker_from_option_cusip(cusip):
    match = re.match(r'^([A-Za-z]+)', cusip)
    return match.group(1) if match else '???'


def get_securities_map(securities):
    # ofx.securities[40].spec.keys() dict_keys(['secinfo', 'opttype', 'strikeprice', 'dtexpire', 'shperctrct', 'secid', 'assetclass', 'fiassetclass'])
    # ofx.securities[40].secinfo.secname 'XLK Jan 31 2025 240.00 Call'
    # ofx.securities[40].secinfo.secid.uniqueid 'XLK250131C00240000'
    # ofx.securities[40].secinfo.ticker 'XLK   250131C00240000'
    # ofx.securities[40].opttype 'CALL'
    # ofx.securities[40].strikeprice Decimal('240.00')
    # ofx.securities[40].dtexpire datetime.datetime(2025, 1, 31, 12, 0, tzinfo=<UTC>)
    # ofx.securities[40].shperctrct 100
    # ofx.securities[40].secid None
    sec_map = {}
    for s in securities:
        sec_map[s.secinfo.secid.uniqueid] = {
            'ticker': s.ticker,
            'name': s.secname,
            'cusip': s.secinfo.secid.uniqueid,
            'uniqueidtype': s.secinfo.secid.uniqueidtype
        }
    return sec_map


def convert_buy_sell_stock(tx, securities_map):
    # <BUYSTOCK(invbuy=<INVBUY(invtran=<INVTRAN(fitid='4ZW30539-20240130-1', dttrade=datetime.datetime(2024, 1, 30, 12, 0, tzinfo=<UTC>), dtsettle=datetime.datetime(2024, 2, 1, 12, 0, tzinfo=<UTC>))>, secid=<SECID(uniqueid='81369Y80', uniqueidtype='CUSIP')>, units=Decimal('5.0000'), unitprice=Decimal('201.9050'), commission=Decimal('0.0000'), total=Decimal('-1009.5300'), subacctsec='MARGIN', subacctfund='MARGIN')>, buytype='BUY')>
    # <SELLSTOCK(invsell=<INVSELL(invtran=<INVTRAN(fitid='4ZW30539-20231108-1', dttrade=datetime.datetime(2023, 11, 8, 12, 0, tzinfo=<UTC>), dtsettle=datetime.datetime(2023, 11, 10, 12, 0, tzinfo=<UTC>))>, secid=<SECID(uniqueid='78464A87', uniqueidtype='CUSIP')>, units=Decimal('-10.0000'), unitprice=Decimal('69.3510'), commission=Decimal('0.0000'), total=Decimal('693.4900'), subacctsec='MARGIN', subacctfund='MARGIN')>, selltype='SELL')>
    sec = securities_map[tx.uniqueid] if tx.uniqueid in securities_map.keys() else unknown_security
    inv = tx.invbuy if type(tx) == BUYSTOCK else tx.invsell
    return {
        'date': tx.dttrade,
        'tx_type': 'BUY' if type(tx) == BUYSTOCK else 'SELL',
        'sec_name': sec['name'],
        'amount': 0 - (inv.units * inv.unitprice),
        'ticker': sec['ticker'],
        'units': inv.units,
        'unit_price': inv.unitprice
    }


def convert_option_transaction(tx):
    # <SELLOPT(invsell=<INVSELL(invtran=<INVTRAN(fitid='4ZU71131-20250204-4', dttrade=datetime.datetime(2025, 2, 4, 12, 0, tzinfo=<UTC>), dtsettle=datetime.datetime(2025, 2, 5, 12, 0, tzinfo=<UTC>))>, secid=<SECID(uniqueid='NOW250321C01040000', uniqueidtype='CUSIP')>, units=Decimal('-1.0000'), unitprice=Decimal('31.0100'), commission=Decimal('0.5000'), fees=Decimal('0.1300'), total=Decimal('3100.3700'), subacctsec='MARGIN', subacctfund='MARGIN')>, optselltype='SELLTOOPEN', shperctrct=100)>
    inv = tx.invbuy if type(tx) == BUYOPT else tx.invsell
    return {
        'date': inv.dttrade,
        'tx_type': tx.optbuytype if type(tx) == BUYOPT else tx.optselltype,
        'name': inv.secid.uniqueid,
        'amount': inv.total,
        'ticker': get_ticker_from_option_cusip(inv.secid.uniqueid),
        'units': inv.units,
        'unit_price': inv.unitprice
    }


def convert_option_closure(tx):
    # <CLOSUREOPT(invtran=<INVTRAN(fitid='4ZU71131-20250214-4', dttrade=datetime.datetime(2025, 2, 14, 12, 0, tzinfo=<UTC>), dtsettle=datetime.datetime(2025, 2, 14, 12, 0, tzinfo=<UTC>))>, secid=<SECID(uniqueid='XLY250214C00227500', uniqueidtype='CUSIP')>, optaction='EXPIRE', units=Decimal('1.0000'), shperctrct=100, subacctsec='MARGIN')>
    return {
        'date': tx.invtran.dttrade,
        'tx_type': tx.optaction,
        'name': tx.secid.uniqueid,
        'amount': 0,
        'ticker': get_ticker_from_option_cusip(tx.secid.uniqueid),
        'units': tx.units,
        'unit_price': 0
    }

def convert_income(tx, securities_map):
    # <INCOME(invtran=<INVTRAN(fitid='4ZW30539-20230428-1', dttrade=datetime.datetime(2023, 4, 28, 12, 0, tzinfo=<UTC>))>, secid=<SECID(uniqueid='46090E10', uniqueidtype='CUSIP')>, incometype='DIV', total=Decimal('1.8900'), subacctsec='MARGIN', subacctfund='MARGIN')>
    sec = securities_map[tx.uniqueid] if tx.uniqueid in securities_map.keys() else unknown_security
    return {
        'date': tx.dttrade,
        'tx_type': tx.incometype,
        'sec_name': sec['name'],
        'amount': tx.total,
        'ticker': sec['ticker'],
        'units': 0,
        'unit_price': 0
    }


def convert_bank_transaction(tx):
    # <INVBANKTRAN(stmttrn=<STMTTRN(trntype='CREDIT', dtposted=datetime.datetime(2023, 12, 11, 12, 0, tzinfo=<UTC>), trnamt=Decimal('0.15'), fitid='4ZW30539-20231211-1', name='FULLYPAID LENDING REBATE')>, subacctfund='CASH')>
    return {
        'date': tx.stmttrn.dtposted,
        'tx_type': tx.stmttrn.trntype,
        'name': tx.stmttrn.name,
        'amount': tx.stmttrn.trnamt,
        'ticker': '',
        'units': 0,
        'unit_price': 0
    }



def convert_to_csv_row(tx, securities_map):
    tx_type = type(tx)
    if tx_type == BUYSTOCK or tx_type == SELLSTOCK:
        tx_out = convert_buy_sell_stock(tx, securities_map)
    elif tx_type == INCOME:
        tx_out = convert_income(tx, securities_map)
    elif tx_type == INVBANKTRAN:
        tx_out = convert_bank_transaction(tx)
    elif tx_type == SELLOPT or tx_type == BUYOPT:
        tx_out = convert_option_transaction(tx)
    elif tx_type == CLOSUREOPT:
        tx_out = convert_option_closure(tx)
    else:
        tx_out = {
            'date': None,
            'tx_type': tx_type,
            'name': '???',
            'amount': 0,
            'ticker': '',
            'units': 0,
            'unit_price': 0
        }
    return tx_out


def get_transactions(ofx, securities_map):
    txs_out = []
    for statement in ofx.statements:
        for tx in statement.transactions:
            # Assuming you want to extract specific transaction fields like date, amount, payee, etc.
            txs_out.append(convert_to_csv_row(tx, securities_map))

    return sorted(txs_out, key=lambda t: t['date'] if 'date' in t else datetime(1970, 1, 1, tzinfo=timezone.utc))


def get_positions(ofx, securities_map):
    # ofx.statements[0].spec.keys() dict_keys(['dtasof', 'curdef', 'invacctfrom', 'invtranlist', 'invposlist', 'invbal', 'invoolist', 'mktginfo', 'inv401k', 'inv401kbal'])
    # ofx.statements[0].dtasof datetime.datetime(2025, 7, 9, 12, 0, tzinfo=<UTC>)
    # ofx.statements[0].invtranlist[0] <BUYSTOCK(invbuy=<INVBUY(invtran=<INVTRAN(fitid='4ZU71131-20250204-1', dttrade=datetime.datetime(2025, 2, 4, 12, 0, tzinfo=<UTC>), dtsettle=datetime.datetime(2025, 2, 5, 12, 0, tzinfo=<UTC>))>, secid=<SECID(uniqueid='02072L56', uniqueidtype='CUSIP')>, units=Decimal('100.0000'), unitprice=Decimal('110.7960'), commission=Decimal('0.0000'), total=Decimal('-11079.6000'), subacctsec='MARGIN', subacctfund='MARGIN')>, buytype='BUY')>
    # ofx.statements[0].invposlist[0].spec.keys() dict_keys(['invpos', 'unitsstreet', 'unitsuser', 'reinvdiv'])
    # ofx.statements[0].invposlist[0].invpos.spec.keys() dict_keys(['secid', 'heldinacct', 'postype', 'units', 'unitprice', 'mktval', 'avgcostbasis', 'dtpriceasof', 'currency', 'memo', 'inv401ksource'])
    # ofx.statements[0].invposlist[0].invpos.secid <SECID(uniqueid='00123Q10', uniqueidtype='CUSIP')>
    # ofx.statements[0].invposlist[0].invpos.postype LONG
    # ofx.statements[0].invposlist[0].invpos.units Decimal('300.0000')
    # ofx.statements[0].invposlist[0].invpos.unitprice Decimal('9.4200')
    # ofx.statements[0].invposlist[0].invpos.mktval Decimal('2826.0000')
    # ofx.statements[0].invposlist[0].invpos.avgcostbasis None
    # ofx.statements[0].positions[0].spec.keys() dict_keys(['invpos', 'unitsstreet', 'unitsuser', 'reinvdiv'])
    # ofx.statements[0].positions[0].invpos.spec.keys() dict_keys(['secid', 'heldinacct', 'postype', 'units', 'unitprice', 'mktval', 'avgcostbasis', 'dtpriceasof', 'currency', 'memo', 'inv401ksource'])
    positions = []
    for ofx_statement in ofx.statements:
        for ofx_position in ofx_statement.positions:
            security = securities_map[ofx_position.invpos.secid.uniqueid]
            position = {
                'secid': ofx_position.invpos.secid.uniqueid,
                'ticker': security['ticker'],
                'name': security['name'],
                'type': ofx_position.invpos.postype,
                'units': ofx_position.invpos.units,
                'unit_price': ofx_position.invpos.unitprice,
                'market_value': ofx_position.invpos.mktval
            }
            positions.append(position)
    return positions


def to_csv(transactions, file_name):
    # Write transaction data as CSV
    with open(file_name, 'w', newline='', encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(transactions[0].keys())  # CSV header
        writer.writerows(tx.values() for tx in transactions)


def custom_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, Decimal):
        return str(obj)
    raise TypeError(f"Type {type(obj)} not serializable")


def to_json(transactions, file_name):
    with open(file_name, "w", encoding="utf-8") as jsonfile:
        json.dump(transactions, jsonfile, default = custom_serializer, indent = 2)


def parse_ofx(qfx_file):
    # Load QFX file
    ofx_tree = OFXTree()
    ofx_tree.parse(qfx_file)
    return ofx_tree.convert()


def add_properties(positions, dividends_earned):

    def get_value(i, first_choice, second_choice):
        return i.get(first_choice) if first_choice in i.keys() else i.get(second_choice)

    one_year_ago = datetime.now(pytz.timezone("America/New_York")) - timedelta(days=365)
    tickers = " ".join(pos["ticker"] for pos in positions)
    yf_tickers = yf.Tickers(tickers)
    for position in positions:
        info = yf_tickers.tickers[position['ticker']].info
        position['sector'] = get_value(info, 'sector', 'category')
        position['industry'] = get_value(info, 'industry', 'category')
        position['beta'] = info.get('beta')
        position['dividend_yield'] = info.get('dividendYield')
        position['dividend_ex_date'] = datetime.fromtimestamp(info.get('exDividendDate')) if 'exDividendDate' in info.keys() else ''
        position['dividends_earned'] = dividends_earned[position['ticker']]['dividends'] if position['ticker'] in dividends_earned else 0
        try:
            div_data = yf_tickers.tickers[position['ticker']].dividends
            divs_ttm = div_data[div_data.index > one_year_ago]
            print(position['ticker'], divs_ttm.size)
            est_next_div_ex_date = divs_ttm.keys()[divs_ttm.size - 1] + pd.DateOffset(months= 12 / divs_ttm.size)
            position['next_ex_div_days'] = (est_next_div_ex_date - pd.Timestamp.now(tz='America/New_York')).days
            position['next_ex_div_date'] = est_next_div_ex_date
            position['dividend_ttm_count'] = divs_ttm.size
            position['dividend_last_amount'] = float(divs_ttm.iloc[divs_ttm.size - 1])
        except RuntimeError:
            print(f'Something went wrong with {position["ticker"]}')
            print(traceback.format_exc())
        position['quote_type'] = info.get('quoteType')
        position['display_name'] = get_value(info, 'displayName', 'shortName')
        position['analyst_recommendation_mean'] = info.get('recommendationMean')
        position['analyst_recommendation'] = info.get('recommendationKey')
        position['analyst_average_rating'] = info.get('averageAnalystRating')



    return positions


def calculate_dividends(transactions):
    summary = {}
    for transaction in transactions:
        ticker = transaction['ticker']
        if ticker == "":
            continue
        if transaction['tx_type'] in ['DIV']:
            if ticker not in summary:
                summary[ticker] = {
                    'dividends': Decimal(0)
                }
            summary[ticker]['dividends'] += Decimal(transaction['amount'])
    return summary


def group_by(factor, positions):
    total_market_value = sum(pos['market_value'] for pos in positions)
    groupings = {}
    for pos in positions:
        grouping_name = pos[factor]

        if grouping_name not in groupings:
            groupings[grouping_name] = {
                'market_value': Decimal(0.0),
                'weighted_div_yield_numerator': Decimal(0.0)
            }
        market_value = pos['market_value']
        groupings[grouping_name]['market_value'] += market_value
        if 'beta' in pos and not pos['beta'] is None:
            if 'weighted_beta_numerator' not in groupings[grouping_name]:
                groupings[grouping_name]['weighted_beta_numerator'] = Decimal(0.0)
            groupings[grouping_name]["weighted_beta_numerator"] += market_value * Decimal(pos['beta'])
        groupings[grouping_name]['weighted_div_yield_numerator'] += market_value * Decimal(pos['dividend_yield'])

    for grouping_name, grouping in groupings.items():
        groupings[grouping_name]['portfolio_share'] = grouping['market_value'] / total_market_value
        grouping['dividend_yield'] = grouping['weighted_div_yield_numerator'] / grouping['market_value']
        grouping.pop('weighted_div_yield_numerator')
        if 'weighted_beta_numerator' in grouping:
            grouping['beta'] = grouping['weighted_beta_numerator'] / grouping['market_value']
            grouping.pop('weighted_beta_numerator')

    return groupings


def calc_group_by(positions):
    return {
        'by_sector' : group_by('sector', positions),
        'by_industry': group_by('industry', positions),
        'by_instrument': group_by('quote_type', positions)
    }


def main():
    requests_cache.install_cache('yfinance_cache', expire_after=600)
    parser = argparse.ArgumentParser()
    parser.add_argument('input_file', help='Path to the QFX file to use as input.')
    args = parser.parse_args()
    qfx_file = args.input_file
    name = os.path.splitext(os.path.basename(qfx_file))[0]
    dir_name = os.path.dirname(qfx_file)
    ofx = parse_ofx(qfx_file)
    securities_map = get_securities_map(ofx.securities)
    transactions = get_transactions(ofx, securities_map)
    to_csv(transactions, os.path.join(dir_name, f"{name}_transactions.csv"))
    to_json(transactions, os.path.join(dir_name, f"{name}_transactions.json"))
    positions = get_positions(ofx, securities_map)
    dividends = calculate_dividends(transactions)
    positions = add_properties(positions, dividends)
    to_csv(positions, os.path.join(dir_name, f"{name}_positions.csv"))
    to_json(positions, os.path.join(dir_name, f"{name}_positions.json"))
    summary = calc_group_by(positions)
    to_json(summary, os.path.join(dir_name, f"{name}_summary.json"))
    to_csv([{'sector': key, **value} for key, value in summary['by_sector'].items()], os.path.join(dir_name, f'{name}_grouped_by_sector.csv'))


if __name__ == '__main__':
    main()

