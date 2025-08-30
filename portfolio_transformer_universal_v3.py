"""
Enhanced Portfolio Transformer Module - Complete Version with All Features
Includes delivery calculations, per-expiry sheets, grouping, and price alerts
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Union
import logging
from dataclasses import dataclass
import yfinance as yf
import warnings
import re
import os

# Suppress warnings
warnings.filterwarnings("ignore", category=FutureWarning)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class Position:
    """Data class for position information"""
    underlying_ticker: str
    symbol: str
    bloomberg_ticker: str
    series: str
    expiry: datetime
    strike: float
    option_type: str
    position: float
    lot_size: int
    security_type: str
    deliverable: float
    underlying_price: Optional[float] = None
    override_price: Optional[float] = None
    bbg_price: Optional[float] = None


class EnhancedPortfolioTransformer:
    """Main transformer class for portfolio positions"""
    
    def __init__(self, fund_name: str):
        self.fund_name = fund_name
        self.mapping_data = {}
        self.positions = []
        self.unmapped_positions = []
        self.underlying_prices = {}
        self.price_overrides = {}
        self.bbg_price_overrides = {}
        self.input_format = None
    
    def get_summary_stats(self):
        """Get summary statistics for the portfolio"""
        total_positions = len(self.positions)
        unmapped_count = len(self.unmapped_positions)
        
        unmapped_symbols = []
        if self.unmapped_positions:
            unmapped_symbols = list(set(pos.get('symbol', '') for pos in self.unmapped_positions))
        
        positions_by_type = {}
        if self.positions:
            for pos in self.positions:
                if hasattr(pos, 'security_type'):
                    pos_type = pos.security_type
                    if pos_type not in positions_by_type:
                        positions_by_type[pos_type] = 0
                    positions_by_type[pos_type] += 1
        
        underlyings = set()
        if self.positions:
            for pos in self.positions:
                if hasattr(pos, 'underlying_ticker'):
                    underlyings.add(pos.underlying_ticker)
        
        return {
            'total_positions': total_positions,
            'total_underlyings': len(underlyings),
            'total_deliverables': sum(1 for p in self.positions if p.deliverable != 0),
            'positions_by_type': positions_by_type,
            'underlyings_list': sorted(list(underlyings)),
            'input_format': self.input_format if hasattr(self, 'input_format') else 'unknown',
            'unmapped_count': unmapped_count,
            'unmapped_symbols': sorted(unmapped_symbols)
        }
    
    def load_mapping_data(self, mapping_file_path: str = "futures mapping.csv") -> None:
        """Load symbol mapping from CSV file"""
        try:
            if not os.path.exists(mapping_file_path):
                logger.warning(f"Mapping file not found: {mapping_file_path}")
                return
            
            df = pd.read_csv(mapping_file_path)
            df.columns = df.columns.str.strip()
            
            if len(df.columns) >= 3:
                df = df.iloc[:, :3]
                df.columns = ['Symbol', 'Ticker', 'Cash']
            
            for _, row in df.iterrows():
                if pd.notna(row['Symbol']) and pd.notna(row['Ticker']):
                    symbol = str(row['Symbol']).strip()
                    ticker = str(row['Ticker']).strip()
                    cash = str(row['Cash']).strip() if pd.notna(row['Cash']) else f"{ticker} IS Equity"
                    
                    self.mapping_data[symbol] = {
                        'ticker': ticker,
                        'cash_ticker': cash
                    }
            
            logger.info(f"✅ Loaded {len(self.mapping_data)} symbol mappings")
            
        except Exception as e:
            logger.error(f"Error loading mapping file: {str(e)}")
    
    def load_positions(self, input_file_path: str, start_row: int = 12) -> None:
        """Auto-detect input format and load positions"""
        try:
            file_ext = os.path.splitext(input_file_path.lower())[1]
            
            if file_ext == '.csv':
                self.input_format = 'csv_contract_id'
                self._load_csv_positions(input_file_path)
            elif file_ext in ['.xlsx', '.xls']:
                try:
                    df_raw = pd.read_excel(input_file_path, header=None, nrows=50)
                except:
                    df_raw = pd.read_excel(input_file_path, header=None)
                
                detected_format = self._detect_excel_format(df_raw, input_file_path)
                logger.info(f"Detected format: {detected_format}")
                
                if detected_format == 'excel_ms_position':
                    self.input_format = 'excel_ms_position'
                    self._load_ms_position_positions(input_file_path)
                elif detected_format == 'excel_contract_id':
                    self.input_format = 'excel_contract_id'
                    self._load_excel_contract_positions(input_file_path)
                else:
                    self.input_format = 'excel_bod'
                    self._load_bod_positions(input_file_path, start_row)
            
            logger.info(f"✅ Loaded {len(self.positions)} positions, {len(self.unmapped_positions)} unmapped")
            
        except Exception as e:
            logger.error(f"Error loading positions: {str(e)}")
            raise
    
    def _detect_excel_format(self, df_raw: pd.DataFrame, file_path: str) -> str:
        """Detect Excel file format"""
        logger.info(f"Analyzing file with shape: {df_raw.shape}")
        
        # Check for MS Position format
        if len(df_raw.columns) >= 22:
            ms_position_indicators = 0
            for row_idx in range(min(50, len(df_raw))):
                try:
                    col1_val = str(df_raw.iloc[row_idx, 0]).strip() if pd.notna(df_raw.iloc[row_idx, 0]) else ""
                    if col1_val and '-' in col1_val:
                        parts = col1_val.split('-')
                        if len(parts) >= 3 and parts[0] in ['FUTSTK', 'OPTSTK']:
                            ms_position_indicators += 1
                            if ms_position_indicators >= 2:
                                logger.info(f"MS Position format detected")
                                return 'excel_ms_position'
                except:
                    continue
        
        # Check for BOD format
        if len(df_raw.columns) >= 16:
            for row_idx in range(5, min(30, len(df_raw))):
                if len(df_raw.iloc[row_idx]) < 16:
                    continue
                col2_val = str(df_raw.iloc[row_idx, 1]).strip() if pd.notna(df_raw.iloc[row_idx, 1]) else ""
                if col2_val and len(col2_val) >= 2 and col2_val.replace('&', '').replace('-', '').isalpha():
                    if col2_val.upper() == col2_val:
                        logger.info(f"BOD format detected")
                        return 'excel_bod'
        
        # Check filename hints
        file_lower = file_path.lower()
        if 'ms' in file_lower and 'position' in file_lower:
            return 'excel_ms_position'
        
        return 'excel_bod'
    
    def _parse_contract_id(self, contract_id: str) -> Optional[Dict]:
        """Parse contract ID string to extract components"""
        try:
            contract_id = contract_id.strip()
            parts = contract_id.split('-')
            
            if len(parts) < 3:
                return None
            
            contract_type = parts[0].strip()
            symbol = parts[1].strip()
            date_str = parts[2].strip()
            
            option_type = ''
            strike = 0.0
            
            if contract_type == 'OPTSTK' and len(parts) >= 5:
                option_type = parts[3].strip()
                strike_str = parts[4].strip()
                try:
                    strike = float(strike_str)
                except:
                    strike = 0.0
            
            expiry = self._parse_date_string(date_str)
            series = contract_type
            
            return {
                'symbol': symbol,
                'expiry': expiry,
                'option_type': option_type,
                'strike': strike,
                'series': series,
                'contract_type': contract_type
            }
            
        except Exception as e:
            logger.debug(f"Could not parse contract: {contract_id}")
            return None
    
    def _parse_date_string(self, date_str: str) -> datetime:
        """Parse date string like '28AUG2025' to datetime"""
        try:
            date_str = date_str.strip().upper().replace('-', '')
            
            month_map = {
                'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
                'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
            }
            
            # Try format: 28AUG2025
            match = re.match(r'(\d{1,2})([A-Z]{3})(\d{4})', date_str)
            if match:
                day = int(match.group(1))
                month_abbr = match.group(2)
                year = int(match.group(3))
                
                month = month_map.get(month_abbr)
                if month:
                    return datetime(year, month, day)
            
            # Try format: 28AUG25
            match = re.match(r'(\d{1,2})([A-Z]{3})(\d{2})', date_str)
            if match:
                day = int(match.group(1))
                month_abbr = match.group(2)
                year = 2000 + int(match.group(3))
                
                month = month_map.get(month_abbr)
                if month:
                    return datetime(year, month, day)
            
            return pd.to_datetime(date_str)
            
        except Exception as e:
            logger.warning(f"Could not parse date {date_str}")
            return datetime.now() + timedelta(days=30)
    
    def _load_ms_position_positions(self, excel_file_path: str) -> None:
        """Load positions from MS Position sheet Excel format"""
        try:
            df = pd.read_excel(excel_file_path, header=None)
            logger.info(f"MS Position file loaded: {df.shape[0]} rows, {df.shape[1]} columns")
            
            positions = []
            unmapped = []
            processed = 0
            skipped = 0
            
            for idx in range(len(df)):
                try:
                    if pd.isna(df.iloc[idx, 0]):
                        continue
                    
                    contract_id = str(df.iloc[idx, 0]).strip()
                    
                    if not contract_id or '-' not in contract_id:
                        continue
                    
                    if not (contract_id.startswith('FUTSTK') or contract_id.startswith('OPTSTK')):
                        continue
                    
                    # Get position value
                    position_val = 0
                    if len(df.columns) > 21:
                        try:
                            position_val = float(df.iloc[idx, 21])
                        except:
                            pass
                    
                    if position_val == 0:
                        for col_idx in [15, 16, 17, 18, 19, 20, 22, 23, 24]:
                            if col_idx < len(df.columns):
                                try:
                                    val = float(df.iloc[idx, col_idx])
                                    if val != 0 and not pd.isna(val):
                                        position_val = val
                                        break
                                except:
                                    continue
                    
                    if position_val == 0:
                        skipped += 1
                        continue
                    
                    parsed = self._parse_contract_id(contract_id)
                    if not parsed:
                        continue
                    
                    symbol = parsed['symbol']
                    
                    # Get lot size
                    lot_size = 1
                    for col_idx in [5, 6, 7, 4]:
                        if col_idx < len(df.columns):
                            try:
                                val = float(df.iloc[idx, col_idx])
                                if val in [1, 25, 50, 75, 100, 125, 150, 200, 250, 500, 1000, 1200, 1250, 1500, 2000, 2500, 3000]:
                                    lot_size = int(val)
                                    break
                            except:
                                continue
                    
                    if symbol not in self.mapping_data:
                        unmapped.append({
                            'symbol': symbol,
                            'contract_id': contract_id,
                            'position': position_val,
                            'lot_size': lot_size,
                            'series': parsed['series'],
                            'expiry': parsed['expiry'],
                            'strike': parsed['strike'],
                            'option_type': parsed['option_type'],
                            'row_number': idx + 1,
                            'source': 'MS Position Format'
                        })
                        continue
                    
                    mapping_info = self.mapping_data[symbol]
                    cash_ticker = mapping_info['cash_ticker']
                    fo_ticker = mapping_info['ticker']
                    
                    # Determine security type
                    if parsed['series'] == 'FUTSTK':
                        security_type = 'Futures'
                    elif parsed['series'] == 'OPTSTK':
                        if parsed['option_type'] in ['CE', 'C']:
                            security_type = 'Call'
                        elif parsed['option_type'] in ['PE', 'P']:
                            security_type = 'Put'
                        else:
                            security_type = 'Option'
                    else:
                        security_type = 'Unknown'
                    
                    # Generate Bloomberg ticker
                    bloomberg_ticker = f"{fo_ticker} {parsed['expiry'].strftime('%m/%d/%y')}"
                    if security_type in ['Call', 'Put']:
                        opt_type = 'C' if security_type == 'Call' else 'P'
                        bloomberg_ticker += f" {opt_type}{parsed['strike']}"
                    
                    position = Position(
                        underlying_ticker=cash_ticker,
                        symbol=symbol,
                        bloomberg_ticker=bloomberg_ticker,
                        series=parsed['series'],
                        expiry=parsed['expiry'],
                        strike=parsed['strike'],
                        option_type=parsed['option_type'],
                        position=position_val,
                        lot_size=lot_size,
                        security_type=security_type,
                        deliverable=0.0
                    )
                    
                    positions.append(position)
                    processed += 1
                    
                except Exception as e:
                    logger.debug(f"Error on row {idx + 1}: {str(e)}")
                    continue
            
            self.positions = positions
            self.unmapped_positions = unmapped
            
            logger.info(f"MS Position Processing Complete: {processed} processed, {skipped} skipped, {len(unmapped)} unmapped")
            
        except Exception as e:
            logger.error(f"Error loading MS Position file: {str(e)}")
            self.positions = []
    
    def _load_csv_positions(self, csv_file_path: str) -> None:
        """Load positions from CSV format"""
        # Simplified implementation - customize as needed
        self.positions = []
        self.unmapped_positions = []
    
    def _load_excel_contract_positions(self, excel_file_path: str) -> None:
        """Load positions from Excel with Contract ID format"""
        # Simplified implementation - customize as needed
        self.positions = []
        self.unmapped_positions = []
    
    def _load_bod_positions(self, bod_file_path: str, start_row: int = 12) -> None:
        """Load BOD positions from Excel file"""
        try:
            df = pd.read_excel(bod_file_path, header=None)
            
            data_start_row = start_row
            for i in range(min(30, len(df))):
                if len(df.iloc[i]) >= 16:
                    col2_val = str(df.iloc[i, 1]).strip() if pd.notna(df.iloc[i, 1]) else ""
                    if col2_val and col2_val in self.mapping_data:
                        data_start_row = i
                        break
            
            data_df = df.iloc[data_start_row:].copy()
            positions = []
            unmapped = []
            
            for idx, row in data_df.iterrows():
                if len(row) < 16 or pd.isna(row.iloc[1]):
                    continue
                
                try:
                    symbol = str(row.iloc[1]).strip()
                    series = str(row.iloc[2]).strip() if pd.notna(row.iloc[2]) else 'EQ'
                    expiry_raw = row.iloc[3]
                    strike = float(row.iloc[4]) if pd.notna(row.iloc[4]) else 0.0
                    option_type = str(row.iloc[5]).strip() if pd.notna(row.iloc[5]) else ''
                    lot_size = int(row.iloc[6]) if pd.notna(row.iloc[6]) else 1
                    open_position = float(row.iloc[15]) if pd.notna(row.iloc[15]) else 0.0
                    
                    if open_position == 0:
                        continue
                    
                    if pd.notna(expiry_raw):
                        expiry = pd.to_datetime(expiry_raw)
                    else:
                        expiry = datetime.now() + timedelta(days=30)
                    
                    if symbol not in self.mapping_data:
                        unmapped.append({
                            'symbol': symbol,
                            'position': open_position,
                            'lot_size': lot_size,
                            'row_number': idx + 1,
                            'source': 'BOD Format'
                        })
                        continue
                    
                    mapping_info = self.mapping_data[symbol]
                    cash_ticker = mapping_info['cash_ticker']
                    fo_ticker = mapping_info['ticker']
                    
                    if series == 'FUTSTK':
                        security_type = 'Futures'
                    elif series == 'OPTSTK' and option_type == 'CE':
                        security_type = 'Call'
                    elif series == 'OPTSTK' and option_type == 'PE':
                        security_type = 'Put'
                    else:
                        security_type = 'Equity'
                    
                    bloomberg_ticker = f"{fo_ticker} {expiry.strftime('%m/%d/%y')}"
                    if security_type in ['Call', 'Put']:
                        bloomberg_ticker += f" C{strike}" if security_type == 'Call' else f" P{strike}"
                    
                    position = Position(
                        underlying_ticker=cash_ticker,
                        symbol=symbol,
                        bloomberg_ticker=bloomberg_ticker,
                        series=series,
                        expiry=expiry,
                        strike=strike,
                        option_type=option_type,
                        position=open_position,
                        lot_size=lot_size,
                        security_type=security_type,
                        deliverable=0.0
                    )
                    
                    positions.append(position)
                    
                except Exception as e:
                    logger.warning(f"Error processing row {idx}: {str(e)}")
                    continue
            
            self.positions = positions
            self.unmapped_positions = unmapped
            
        except Exception as e:
            logger.error(f"Error loading BOD file: {str(e)}")
    
    def calculate_deliverables(self, auto_fetch_prices: bool = True) -> None:
        """Calculate deliverable positions"""
        if auto_fetch_prices:
            self.fetch_underlying_prices()
        
        for position in self.positions:
            try:
                system_price = self.underlying_prices.get(position.symbol)
                position.underlying_price = system_price
                
                if position.security_type == "Futures":
                    position.deliverable = position.position
                elif position.security_type in ["Call", "Put"]:
                    if system_price is None:
                        position.deliverable = position.position
                    else:
                        is_itm = self._is_in_the_money(position.option_type, position.strike, system_price)
                        if is_itm:
                            position.deliverable = position.position if position.security_type == "Call" else -position.position
                        else:
                            position.deliverable = 0.0
                else:
                    position.deliverable = 0.0
            except:
                position.deliverable = 0.0
    
    def _is_in_the_money(self, option_type: str, strike: float, spot_price: float) -> bool:
        """Check if option is in the money"""
        if option_type in ['CE', 'Call', 'C']:
            return spot_price > strike
        elif option_type in ['PE', 'Put', 'P']:
            return spot_price < strike
        return False
    
    def fetch_underlying_prices(self, symbols: List[str] = None) -> Dict[str, float]:
        """Fetch prices from Yahoo Finance"""
        if symbols is None:
            symbols = list(set(pos.symbol for pos in self.positions))
        
        logger.info(f"Fetching prices for {len(symbols)} symbols...")
        prices = {}
        
        for symbol in symbols:
            try:
                yahoo_symbols = [f"{symbol}.NS", f"{symbol}.BO", symbol]
                for yahoo_symbol in yahoo_symbols:
                    try:
                        ticker = yf.Ticker(yahoo_symbol)
                        hist = ticker.history(period="1d")
                        if not hist.empty:
                            prices[symbol] = float(hist['Close'].iloc[-1])
                            break
                    except:
                        continue
            except:
                pass
        
        self.underlying_prices.update(prices)
        logger.info(f"Fetched {len(prices)} prices")
        return prices
    
    def _create_price_alert_sheet(self, workbook, positions: List) -> None:
        """Create price alert sheet for positions near strike price"""
        from openpyxl.styles import Font, PatternFill, Alignment
        from openpyxl.utils import get_column_letter
        
        ws = workbook.create_sheet(title="Price_Alerts", index=1)
        
        # Add threshold input cell
        ws.cell(row=1, column=1, value="Alert Threshold (%):")
        ws.cell(row=1, column=1).font = Font(bold=True)
        ws.cell(row=1, column=2, value=1.0)
        ws.cell(row=1, column=2).fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")
        
        # Headers
        headers = [
            'Underlying', 'Symbol', 'Type', 'Strike', 'Current Price',
            'Moneyness %', 'Days to Expiry', 'Position', 'Alert Status'
        ]
        
        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=3, column=col, value=header)
            cell.font = Font(bold=True, size=11, color="FFFFFF")
            cell.fill = PatternFill(start_color="FF6600", end_color="FF6600", fill_type="solid")
        
        # Filter for options only
        options_positions = [pos for pos in positions if pos.security_type in ['Call', 'Put']]
        
        current_row = 4
        today = datetime.now()
        
        for position in sorted(options_positions, key=lambda x: (x.underlying_ticker, x.expiry)):
            ws.cell(row=current_row, column=1, value=position.underlying_ticker)
            ws.cell(row=current_row, column=2, value=position.bloomberg_ticker)
            ws.cell(row=current_row, column=3, value=position.security_type)
            ws.cell(row=current_row, column=4, value=position.strike)
            
            # Current price formula
            price_formula = f"=VLOOKUP(A{current_row},Net_Position_Summary!A:H,8,FALSE)"
            ws.cell(row=current_row, column=5, value=price_formula)
            
            # Moneyness % calculation
            if position.security_type == "Call":
                moneyness_formula = f"=IF(E{current_row}>0,(E{current_row}-D{current_row})/D{current_row}*100,0)"
            else:
                moneyness_formula = f"=IF(E{current_row}>0,(D{current_row}-E{current_row})/D{current_row}*100,0)"
            ws.cell(row=current_row, column=6, value=moneyness_formula)
            
            # Days to expiry
            days_to_expiry = (position.expiry - today).days
            ws.cell(row=current_row, column=7, value=days_to_expiry)
            
            ws.cell(row=current_row, column=8, value=position.position)
            
            # Alert Status
            alert_formula = f'=IF(ABS(F{current_row})<=$B$1,"🔴 NEAR STRIKE",IF(ABS(F{current_row})<=($B$1*2),"🟡 WATCH","🟢 SAFE"))'
            ws.cell(row=current_row, column=9, value=alert_formula)
            
            current_row += 1
        
        # Auto-size columns
        for col in range(1, 10):
            ws.column_dimensions[get_column_letter(col)].width = 15
        
        ws.freeze_panes = ws['A4']
    
    def _create_net_position_summary(self, workbook, positions: List) -> None:
        """Create net position summary sheet"""
        from openpyxl.styles import Font, PatternFill, Alignment
        from openpyxl.utils import get_column_letter
        
        ws = workbook.create_sheet(title="Net_Position_Summary", index=0)
        
        # Headers
        headers = [
            'Underlying', 'Total Contracts', 'Total Lots', 'Lot Size',
            'System Deliverable', 'Override Deliverable', 'BBG Deliverable',
            'System Price', 'Override Price', 'BBG Price'
        ]
        
        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=1, column=col, value=header)
            cell.font = Font(bold=True, size=12, color="FFFFFF")
            cell.fill = PatternFill(start_color="0066CC", end_color="0066CC", fill_type="solid")
            cell.alignment = Alignment(horizontal="center")
        
        # Aggregate positions by underlying
        underlying_summary = {}
        for position in positions:
            underlying = position.underlying_ticker
            if underlying not in underlying_summary:
                underlying_summary[underlying] = {
                    'positions': [],
                    'total_contracts': 0,
                    'total_lots': 0,
                    'lot_size': position.lot_size,
                    'system_price': position.underlying_price
                }
            underlying_summary[underlying]['positions'].append(position)
            underlying_summary[underlying]['total_contracts'] += 1
            underlying_summary[underlying]['total_lots'] += abs(position.position)
        
        # Write summary data
        current_row = 2
        for underlying in sorted(underlying_summary.keys()):
            data = underlying_summary[underlying]
            
            ws.cell(row=current_row, column=1, value=underlying)
            ws.cell(row=current_row, column=2, value=data['total_contracts'])
            ws.cell(row=current_row, column=3, value=data['total_lots'])
            ws.cell(row=current_row, column=4, value=data['lot_size'])
            
            system_deliverable = sum(pos.deliverable for pos in data['positions'])
            ws.cell(row=current_row, column=5, value=system_deliverable)
            
            # Formulas for Override and BBG deliverables
            ws.cell(row=current_row, column=6, value=f"=SUMIF(Master_All_Expiries!A:A,A{current_row},Master_All_Expiries!H:H)")
            ws.cell(row=current_row, column=7, value=f"=SUMIF(Master_All_Expiries!A:A,A{current_row},Master_All_Expiries!L:L)")
            
            ws.cell(row=current_row, column=8, value=data['system_price'])
            ws.cell(row=current_row, column=9, value="")  # Override price
            ws.cell(row=current_row, column=10, value=f'=@BDP(A{current_row},"PX_LAST")')
            
            if abs(system_deliverable) > 100:
                for col in range(1, 11):
                    ws.cell(row=current_row, column=col).fill = PatternFill(
                        start_color="FFE6E6", end_color="FFE6E6", fill_type="solid"
                    )
            
            current_row += 1
        
        # Add totals row
        total_row = current_row + 1
        ws.cell(row=total_row, column=1, value="TOTAL")
        ws.cell(row=total_row, column=1).font = Font(bold=True, size=12)
        
        ws.cell(row=total_row, column=2, value=f"=SUM(B2:B{current_row-1})")
        ws.cell(row=total_row, column=3, value=f"=SUM(C2:C{current_row-1})")
        ws.cell(row=total_row, column=5, value=f"=SUM(E2:E{current_row-1})")
        ws.cell(row=total_row, column=6, value=f"=SUM(F2:F{current_row-1})")
        ws.cell(row=total_row, column=7, value=f"=SUM(G2:G{current_row-1})")
        
        for col in range(1, 11):
            cell = ws.cell(row=total_row, column=col)
            cell.fill = PatternFill(start_color="D9D9D9", end_color="D9D9D9", fill_type="solid")
            cell.font = Font(bold=True)
        
        for col in range(1, 11):
            ws.column_dimensions[get_column_letter(col)].width = 18
        
        ws.freeze_panes = ws['A2']
    
    def _create_grouped_sheet_enhanced(self, workbook, sheet_name: str, positions: List) -> None:
        """Create sheet with grouping by underlying"""
        from openpyxl.styles import Font, PatternFill
        from openpyxl.utils import get_column_letter
        
        ws = workbook.create_sheet(title=sheet_name)
        
        # Headers
        headers = [
            'Underlying', 'Symbol', 'Expiry', 'Position', 'Type', 'Strike', 
            'System Deliverable', 'Override Deliverable', 'System Price', 'Override Price', 
            'BBG Price', 'BBG Deliverable'
        ]
        
        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=1, column=col, value=header)
            cell.font = Font(bold=True, size=12, color="FFFFFF")
            cell.fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
        
        # Group positions by underlying
        grouped_positions = {}
        for position in positions:
            underlying = position.underlying_ticker
            if underlying not in grouped_positions:
                grouped_positions[underlying] = []
            grouped_positions[underlying].append(position)
        
        current_row = 2
        group_ranges = []
        
        for underlying_ticker in sorted(grouped_positions.keys()):
            positions_group = grouped_positions[underlying_ticker]
            underlying_row = current_row
            
            # Write underlying summary row
            ws.cell(row=underlying_row, column=1, value=underlying_ticker)
            
            repr_position = positions_group[0]
            system_price = repr_position.underlying_price
            if system_price is not None:
                system_price = round(system_price, 1)
            ws.cell(row=underlying_row, column=9, value=system_price)
            
            bbg_formula = f'=@BDP(A{underlying_row},"PX_LAST")'
            ws.cell(row=underlying_row, column=11, value=bbg_formula)
            
            for col in range(1, 13):
                cell = ws.cell(row=underlying_row, column=col)
                cell.fill = PatternFill(start_color="B8CCE4", end_color="B8CCE4", fill_type="solid")
                cell.font = Font(bold=True, size=11)
            
            current_row += 1
            detail_start_row = current_row
            
            # Write individual positions
            for position in sorted(positions_group, key=lambda x: (x.expiry, x.strike, x.option_type)):
                ws.cell(row=current_row, column=2, value=position.bloomberg_ticker)
                ws.cell(row=current_row, column=3, value=position.expiry.strftime('%Y-%m-%d'))
                ws.cell(row=current_row, column=4, value=position.position)
                ws.cell(row=current_row, column=5, value=position.security_type)
                ws.cell(row=current_row, column=6, value=position.strike if position.strike > 0 else None)
                
                ws.cell(row=current_row, column=9, value=f"=I{underlying_row}")
                ws.cell(row=current_row, column=10, value=f"=J{underlying_row}")
                ws.cell(row=current_row, column=11, value=f"=K{underlying_row}")
                
                # Deliverable formulas
                system_formula = self._make_deliverable_formula(current_row, position, 'I')
                override_formula = self._make_deliverable_formula(current_row, position, 'J')
                bbg_formula = self._make_deliverable_formula(current_row, position, 'K')
                
                ws.cell(row=current_row, column=7, value=system_formula)
                ws.cell(row=current_row, column=8, value=override_formula)
                ws.cell(row=current_row, column=12, value=bbg_formula)
                
                current_row += 1
            
            detail_end_row = current_row - 1
            
            # Add total formulas
            if detail_end_row >= detail_start_row:
                ws.cell(row=underlying_row, column=7, value=f"=SUM(G{detail_start_row}:G{detail_end_row})")
                ws.cell(row=underlying_row, column=8, value=f"=SUM(H{detail_start_row}:H{detail_end_row})")
                ws.cell(row=underlying_row, column=12, value=f"=SUM(L{detail_start_row}:L{detail_end_row})")
                
                group_ranges.append((detail_start_row, detail_end_row))
            else:
                ws.cell(row=underlying_row, column=7, value=0)
                ws.cell(row=underlying_row, column=8, value=0)
                ws.cell(row=underlying_row, column=12, value=0)
        
        # Apply grouping
        if group_ranges:
            try:
                ws.sheet_properties.outlinePr.showOutlineSymbols = True
                ws.sheet_properties.outlinePr.applyStyles = False
                ws.sheet_properties.outlinePr.summaryBelow = False
                ws.sheet_properties.outlinePr.summaryRight = False
                
                for start_row, end_row in group_ranges:
                    if end_row >= start_row:
                        ws.row_dimensions.group(start_row, end_row, hidden=True, outline_level=1)
                
                logger.info(f"Applied grouping to {len(group_ranges)} groups in sheet '{sheet_name}'")
            except Exception as e:
                logger.warning(f"Could not apply grouping: {str(e)}")
        
        for col in range(1, 13):
            ws.column_dimensions[get_column_letter(col)].width = 18
        
        ws.freeze_panes = ws['A2']
    
    def _make_deliverable_formula(self, row: int, position, price_col: str) -> str:
        """Create deliverable calculation formula"""
        if position.security_type == "Futures":
            return f"=D{row}"
        elif position.security_type == "Call":
            return f"=IF({price_col}{row}>F{row},D{row},0)"
        elif position.security_type == "Put":
            return f"=IF({price_col}{row}<F{row},-D{row},0)"
        else:
            return "0"
    
    def _create_unmapped_sheet(self, workbook) -> None:
        """Create sheet for unmapped symbols"""
        from openpyxl.styles import Font, PatternFill
        from openpyxl.utils import get_column_letter
        
        ws = workbook.create_sheet(title="Unmapped_Symbols")
        
        headers = ['Symbol', 'Contract ID', 'Position', 'Lot Size', 'Expiry', 'Strike', 'Type', 'Row Number', 'Source']
        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=1, column=col, value=header)
            cell.font = Font(bold=True, color="FFFFFF")
            cell.fill = PatternFill(start_color="FF6600", end_color="FF6600", fill_type="solid")
        
        current_row = 2
        for unmapped in self.unmapped_positions:
            ws.cell(row=current_row, column=1, value=unmapped.get('symbol', ''))
            ws.cell(row=current_row, column=2, value=unmapped.get('contract_id', ''))
            ws.cell(row=current_row, column=3, value=unmapped.get('position', 0))
            ws.cell(row=current_row, column=4, value=unmapped.get('lot_size', 1))
            
            expiry = unmapped.get('expiry')
            if expiry and isinstance(expiry, datetime):
                ws.cell(row=current_row, column=5, value=expiry.strftime('%Y-%m-%d'))
            
            ws.cell(row=current_row, column=6, value=unmapped.get('strike', 0))
            ws.cell(row=current_row, column=7, value=unmapped.get('option_type', ''))
            ws.cell(row=current_row, column=8, value=unmapped.get('row_number', 0))
            ws.cell(row=current_row, column=9, value=unmapped.get('source', ''))
            current_row += 1
        
        for col in range(1, 10):
            ws.column_dimensions[get_column_letter(col)].width = 15
    
    def save_output_excel(self, output_path: str) -> None:
        """Save complete output to Excel with all sheets"""
        try:
            import openpyxl
            
            logger.info(f"Creating Excel output with {len(self.positions)} positions")
            
            wb = openpyxl.Workbook()
            wb.remove(wb.active)
            
            if self.positions:
                # Create Net Position Summary
                self._create_net_position_summary(wb, self.positions)
                logger.info("Created Net Position Summary")
                
                # Create Price Alert sheet
                self._create_price_alert_sheet(wb, self.positions)
                logger.info("Created Price Alert sheet")
                
                # Create Master sheet with all positions
                self._create_grouped_sheet_enhanced(wb, "Master_All_Expiries", self.positions)
                logger.info("Created Master sheet")
                
                # Group by expiry and create individual sheets
                expiry_groups = {}
                for position in self.positions:
                    expiry_key = position.expiry.strftime('%Y-%m-%d')
                    if expiry_key not in expiry_groups:
                        expiry_groups[expiry_key] = []
                    expiry_groups[expiry_key].append(position)
                
                for expiry_date, positions in sorted(expiry_groups.items()):
                    sheet_name = f"Expiry_{expiry_date.replace('-', '_')}"
                    if len(sheet_name) > 31:
                        sheet_name = f"Exp_{expiry_date.replace('-', '_')}"
                    
                    self._create_grouped_sheet_enhanced(wb, sheet_name, positions)
                    logger.info(f"Created sheet '{sheet_name}' with {len(positions)} positions")
            
            # Create unmapped sheet if needed
            if self.unmapped_positions:
                self._create_unmapped_sheet(wb)
                logger.info(f"Created Unmapped_Symbols sheet with {len(self.unmapped_positions)} positions")
            
            wb.save(output_path)
            logger.info(f"✅ Excel output saved to: {output_path}")
            
        except Exception as e:
            logger.error(f"Error saving Excel: {str(e)}")
            raise
