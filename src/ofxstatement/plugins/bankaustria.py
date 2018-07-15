#!/usr/bin/env python3
# This file is part of ofxstatement-austrian.
# See README.rst for more information.

import csv
import re
from datetime import datetime
from ofxstatement.plugin import Plugin
from ofxstatement.parser import CsvStatementParser
from ofxstatement.statement import generate_transaction_id
from ofxstatement import statement
from ofxstatement.plugins.utils import \
    clean_multiple_whitespaces, fix_amount_string


# TODO
# 1. Check account data, see
#    https://github.com/kedder/ofxstatement/blob/master/src/ofxstatement/ofx.py#L117
#
#   * account.bank_id
#   * account.acct_id
#   * account.acct_type
#   * etc.
#
# 2. Parse more data i.e. BANKOMAT?

class BankAustriaCsvParser(CsvStatementParser):
    """The csv parser for Bank Austria."""

    date_format = "%d.%m.%Y"

    #  0 Buchungsdatum
    #  1 Valutadatum
    #  2 Buchungstext
    #  3 Interne Notiz
    #  4 Waehrung
    #  5 Betrag
    #  6 Belegdaten
    #  7 Belegnummer
    #  8 Auftraggebername
    #  9 Auftraggeberkonto
    # 10 Auftraggeber BLZ
    # 11 Empfaengername
    # 12 Empfaengerkonto
    # 13 Empfaenger BLZ
    # 14 Zahlungsgrund

    mappings = {
        "date": 1,
        "date_user": 0,
        "memo": 14,
        "amount": 5,
        "check_no": 7,
        "payee": 11,
        }

    def parse(self):
        """Parse."""
        stmt = super(BankAustriaCsvParser, self).parse()
        statement.recalculate_balance(stmt)
        return stmt

    def split_records(self):
        """Split records using a custom dialect."""
        return csv.reader(self.fin, delimiter=';', quotechar='"')

    def parse_record(self, line):
        """Parse a single record."""
        # Skip header line
        if self.cur_record == 1:
            return None

        # Fix German number format prior to parsing
        line[5] = format(fix_amount_string(line[5]))  # German number format

        # Create statement
        # Parse line elements using the mappings defined above
        #   (call parse_record() from parent class)
        stmtline = super(BankAustriaCsvParser, self).parse_record(line)

        stmtline.id = generate_transaction_id(stmtline)

        # manual date_user conversion as date_user has wrong format
        # TODO remove me when the following patch was released (v0.6.2?)
        # https://github.com/kedder/ofxstatement/commit/38af84d525f5c47c7fab67c02b36c32dcfc805b3
        stmtline.date_user = datetime.strptime(line[1], self.date_format)

        stmtline.trntype = 'DEBIT' if stmtline.amount < 0 else 'CREDIT'

        # Account id
        # if not self.statement.account_id:
        #     self.statement.account_id = line[9]

        # Currency
        if not self.statement.currency:
            self.statement.currency = line[4]

        # .payee is imported as "Description" in GnuCash
        # .memo is imported as "Notes" in GnuCash
        #
        # When .payee is empty, GnuCash
        # imports .memo to "Description" and keeps "Notes" empty, see
        # https://github.com/archont00/ofxstatement-unicreditcz/blob/master/src/ofxstatement/plugins/unicreditcz.py#L100

        # Fixup Memo, Payee, and TRXTYPE
        if line[2].startswith('POS'):
            stmtline.trntype = 'POS'
            stmtline.memo = self.parsePosAtm(line[2])

        elif line[2].startswith('ATM'):
            stmtline.trntype = 'ATM'
            stmtline.memo = self.parsePosAtm(line[2])

        elif line[2].startswith('AUTOMAT') or line[2].startswith('BANKOMAT'):
            # > AUTOMAT   00011942 K1   14.01. 13:47     O
            # > BANKOMAT  00021241 K4   08.03. 09:43     O
            stmtline.trntype = 'ATM'
            # TODO stmtline.memo = self.parsePosAtm(line[2]) ?
            stmtline.memo = line[2]

        elif line[2].startswith('ABHEBUNG AUTOMAT'):
            # > ABHEBUNG AUTOMAT NR. 14547 AM 31.01. UM 15.53 UHR Fil.ABC BANKCARD 2    # noqa: E501
            # TODO stmtline.memo = self.parsePosAtm(line[2]) ?
            stmtline.trntype = 'ATM'
            stmtline.memo = line[2]

        elif line[2].startswith('EINZAHLUNG'):
            # > EINZAHLUNG AUTOMAT NR. 55145 AM 31.01. / 15.55 UHR Fil.ABC BANKCARD 2 EIGENERLAG    # noqa: E501
            stmtline.memo = line[2]

        elif line[2].startswith('Lastschrift JustinCase'):
            # > Lastschrift JustinCase MRefAT123123123123123123JIC Entgelt für Bank Austria 0,69 enth‰lt 20% Ust., das sind EUR 0,12.   # noqa: E501
            stmtline.memo = line[2]

        elif line[6].startswith('SEPA-AUFTRAGSBESTÄTIGUNG'):
            if not stmtline.memo:
                stmtline.memo = self.parseDocument(line[6])

        elif (line[6].startswith('GUTSCHRIFT') or line[6].startswith('SEPA') or
                line[6].startswith('ÜBERWEISUNG')):
            # Auftraggebername holds the information we want
            stmtline.payee = line[8]
            if not stmtline.memo:
                stmtline.memo = self.parseDocument(line[6])

        else:
            stmtline.memo = line[2]

        # Simple cleanup
        stmtline.payee = clean_multiple_whitespaces(stmtline.payee)
        stmtline.memo = clean_multiple_whitespaces(stmtline.memo)

        # Add Internal Note, if exists
        if line[3]:
            # Add trailing whitespace if memo exists
            if stmtline.memo:
                stmtline.memo = stmtline.memo + ' '
            stmtline.memo = stmtline.memo + '(NOTE: )' + line[3]

        return stmtline

    def parseDocument(self, toparse):
        """Parse Belegdaten"""
        # 123456789x123456789x123456789x123456789x123456789x123456789x123456789x123456789x123456789x123456789x
        # SEPA-AUFTRAGSBESTÄTIGUNG
        # GUTSCHRIFT
        # ÜBERWEISUNG
        # SEPA LASTSCHRIFT
        p = re.compile('.*Belegnr.: ([0-9.]{18}).*(?:Zahlungsempf|Zahlungspfl).: (.{56}).*Zahlungsgrund: (.{105}).*Zahlungsref.: (.{110})')  # noqa: E501
        mm = p.findall(toparse)
        if mm:
            m = mm[0]
            # no = m[0]
            # myname = m[1].strip()
            reason = m[2].strip()
            ref = m[3].strip()

            if reason:
                text = reason
            else:
                text = ref

            memo = '%s' % (text)
        else:
            memo = 'ERR: ' + toparse

        return memo

    def parsePosAtm(self, toparse):
        """Parse POS/ATM Lines"""

        # POS/ATM have a fixed layout in line[2]. Some data can also be found in other columns          # noqa: E501
        # i.e.
        # > 123456789x123456789x123456789x123456789x123456789x123456789x123456789x123456789x123456789x  # noqa: E501
        # > ATM         100,00 AT  K1   15.01. 19:08 O ATM S6EE0275           KLOSTERNEUBUR 4300        # noqa: E501
        # > POS          11,00 NL  K1   16.01. 14:46 O NS SCHIPHOL 216        LUCHTHAVEN SC 1118 AX     # noqa: E501
        #
        # Matches:
        # > 0                1  2   3       4      5 - 6                      7             8           # noqa: E501
        # > TYPE           AMT CC  ##    DATE   TIME O SHOP                   LOCATION      ZIP         # noqa: E501

        p = re.compile('(POS|ATM) +([0-9]+,[0-9]+) ([A-Z]+) +(K[0-9]) +(......) (..:..) O (.{22}) +(.{13}) +(.*)')  # noqa: E501
        mm = p.findall(toparse)
        if mm:
            # ex. result from above
            # ATM: ATM S6EE0275, 4300 KLOSTERNEUBUR, AT; 100,00 EUR on 15.01. 19:08h                    # noqa: E501
            # POS: NS SCHIPHOL 216, 1118 AX LUCHTHAVEN SC, NL; 11,00 EUR on 16.01. 14:46h               # noqa: E501

            m = mm[0]
            memo = '%s: %s, %s %s, %s; %s %s on %s %sh' % (m[0], m[6].strip(), m[8], m[7].strip(), m[2], m[1], self.statement.currency, m[4], m[5])  # noqa: E501
        else:
            memo = 'ERR: ' + toparse

        return memo


class BankAustriaPlugin(Plugin):
    """Bank Austria (CSV)"""

    def get_parser(self, filename):
        """Get a parser instance."""
        encoding = self.settings.get('charset', 'iso-8859-1')
        f = open(filename, 'r', encoding=encoding)
        parser = BankAustriaCsvParser(f)
        parser.statement.bank_id = self.settings.get('bank', 'Bank-Austria')
        return parser

# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4 smartindent autoindent
