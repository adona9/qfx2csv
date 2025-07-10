from ofxtools import OFXTree
from ofxtools.models.invest.transactions import BUYOPT
from ofxtools.models.invest.transactions import BUYSTOCK
from ofxtools.models.invest.transactions import CLOSUREOPT
from ofxtools.models.invest.transactions import SELLOPT
from ofxtools.models.invest.transactions import SELLSTOCK
from ofxtools.models.invest.transactions import INCOME
from ofxtools.models.invest.transactions import INVBANKTRAN

import csv
import sys
import re
import argparse
from datetime import datetime, timezone


# https://ofxtools.readthedocs.io/en/latest/parser.html


unknown_security = {
    'ticker': 'unknown',
    'name': 'unknown'
}


def get_ticker_from_option_cusip(cusip):
    match = re.match(r'^([A-Za-z]+)', cusip)
    return match.group(1) if match else '???'


def get_securities_map(securities):
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
        'units': '',
        'unit_price': ''
    }


def convert_bank_transaction(tx):
    # <INVBANKTRAN(stmttrn=<STMTTRN(trntype='CREDIT', dtposted=datetime.datetime(2023, 12, 11, 12, 0, tzinfo=<UTC>), trnamt=Decimal('0.15'), fitid='4ZW30539-20231211-1', name='FULLYPAID LENDING REBATE')>, subacctfund='CASH')>
    return {
        'date': tx.stmttrn.dtposted,
        'tx_type': tx.stmttrn.trntype,
        'name': tx.stmttrn.name,
        'amount': tx.stmttrn.trnamt,
        'ticker': '',
        'units': '',
        'unit_price': ''
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


def convert_qfx_to_csv(qfx_file, output_file):
    # Load QFX file
    ofx_tree = OFXTree()
    ofx_tree.parse(qfx_file)
    ofx = ofx_tree.convert()
    securities_map = get_securities_map(ofx.securities)
    # Extract transaction data
    txs_out = []
    for statement in ofx.statements:
        for tx in statement.transactions:
            # Assuming you want to extract specific transaction fields like date, amount, payee, etc.
            txs_out.append(convert_to_csv_row(tx, securities_map))

    sorted_txs = sorted(txs_out, key=lambda t: t['date'] if 'date' in t else datetime(1970, 1, 1, tzinfo=timezone.utc))

    # Write transaction data as CSV
    #with open(csv_file, 'w', newline='') as csvfile:
    with output_file as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(sorted_txs[0].keys())  # CSV header
        writer.writerows(tx.values() for tx in sorted_txs)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('input_file', help='Path to the QFX file to use as input.')
    args = parser.parse_args()
    qfx_file = args.input_file
    convert_qfx_to_csv(qfx_file, sys.stdout)


if __name__ == '__main__':
    main()

