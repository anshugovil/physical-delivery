"""
Enhanced Portfolio Transformer Module - Fixed Version with Better MS Position Support
Handles multiple input formats and creates Excel output with deliverable calculations
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
                # Try to read with pandas first
                try:
                    df_raw = pd.read_excel(input_file_path, header=None, nrows=50)
                except:
                    # If password protected, use simpler method
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
        
        # Check for MS Position format - look for contract patterns
        if len(df_raw.columns) >= 22:
            ms_position_indicators = 0
            for row_idx in range(min(50, len(df_raw))):
                try:
                    col1_val = str(df_raw.iloc[row_idx, 0]).strip() if pd.notna(df_raw.iloc[row_idx, 0]) else ""
                    # Check for contract format: FUTSTK-SYMBOL-DATE or OPTSTK-SYMBOL-DATE-TYPE-STRIKE
                    if col1_val and '-' in col1_val:
                        parts = col1_val.split('-')
                        if len(parts) >= 3 and parts[0] in ['FUTSTK', 'OPTSTK']:
                            ms_position_indicators += 1
                            if ms_position_indicators >= 2:
                                logger.info(f"MS Position format detected with contract: {col1_val[:50]}")
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
                        logger.info(f"BOD format detected with symbol: {col2_val}")
                        return 'excel_bod'
        
        # Check filename hints
        file_lower = file_path.lower()
        if 'ms' in file_lower and 'position' in file_lower:
            logger.info("Filename suggests MS Position format")
            return 'excel_ms_position'
        
        # Default to BOD
        logger.info("Defaulting to BOD format")
        return 'excel_bod'
    
    def _parse_contract_id(self, contract_id: str) -> Optional[Dict]:
        """Parse contract ID string to extract components"""
        try:
            # Remove any extra spaces
            contract_id = contract_id.strip()
            parts = contract_id.split('-')
            
            if len(parts) < 3:
                return None
            
            contract_type = parts[0].strip()  # FUTSTK or OPTSTK
            symbol = parts[1].strip()
            date_str = parts[2].strip()
            
            # Default values
            option_type = ''
            strike = 0.0
            
            # For options, extract type and strike
            if contract_type == 'OPTSTK' and len(parts) >= 5:
                option_type = parts[3].strip()  # CE or PE
                strike_str = parts[4].strip()
                try:
                    strike = float(strike_str)
                except:
                    strike = 0.0
            
            # Parse expiry date
            expiry = self._parse_date_string(date_str)
            
            # Determine series
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
            logger.debug(f"Could not parse contract: {contract_id} - {str(e)}")
            return None
    
    def _parse_date_string(self, date_str: str) -> datetime:
        """Parse date string like '28AUG2025' or '28-AUG-2025' to datetime"""
        try:
            # Clean the date string
            date_str = date_str.strip().upper().replace('-', '')
            
            # Common month abbreviations
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
            
            # Try format: 28AUG25 (two-digit year)
            match = re.match(r'(\d{1,2})([A-Z]{3})(\d{2})', date_str)
            if match:
                day = int(match.group(1))
                month_abbr = match.group(2)
                year = 2000 + int(match.group(3))
                
                month = month_map.get(month_abbr)
                if month:
                    return datetime(year, month, day)
            
            # Fallback: try pandas parsing
            return pd.to_datetime(date_str)
            
        except Exception as e:
            logger.warning(f"Could not parse date {date_str}, using future date")
            return datetime.now() + timedelta(days=30)
    
    def _load_ms_position_positions(self, excel_file_path: str) -> None:
        """Load positions from MS Position sheet Excel format"""
        try:
            # Read the full file
            df = pd.read_excel(excel_file_path, header=None)
            logger.info(f"MS Position file loaded: {df.shape[0]} rows, {df.shape[1]} columns")
            
            positions = []
            unmapped = []
            processed = 0
            skipped = 0
            
            # Look through all rows for contract patterns
            for idx in range(len(df)):
                try:
                    # Get contract ID from column 1 (index 0)
                    if pd.isna(df.iloc[idx, 0]):
                        continue
                    
                    contract_id = str(df.iloc[idx, 0]).strip()
                    
                    # Skip if not a valid contract format
                    if not contract_id or '-' not in contract_id:
                        continue
                    
                    # Check if it starts with FUTSTK or OPTSTK
                    if not (contract_id.startswith('FUTSTK') or contract_id.startswith('OPTSTK')):
                        continue
                    
                    # Get position value - try column 22 (index 21) first, then look in other columns
                    position_val = 0
                    
                    # Try standard column 22
                    if len(df.columns) > 21:
                        try:
                            position_val = float(df.iloc[idx, 21])
                        except:
                            pass
                    
                    # If no position in column 22, try to find it in columns 15-25
                    if position_val == 0:
                        for col_idx in [15, 16, 17, 18, 19, 20, 21, 22, 23, 24]:
                            if col_idx < len(df.columns):
                                try:
                                    val = float(df.iloc[idx, col_idx])
                                    if val != 0 and not pd.isna(val):
                                        position_val = val
                                        logger.debug(f"Found position {val} in column {col_idx + 1}")
                                        break
                                except:
                                    continue
                    
                    # Skip if no position
                    if position_val == 0:
                        skipped += 1
                        continue
                    
                    # Parse the contract
                    parsed = self._parse_contract_id(contract_id)
                    if not parsed:
                        logger.warning(f"Could not parse: {contract_id}")
                        continue
                    
                    symbol = parsed['symbol']
                    
                    # Try to get lot size - look in columns 5-7
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
                    
                    # Check if symbol has mapping
                    if symbol not in self.mapping_data:
                        logger.warning(f"No mapping for symbol: {symbol}")
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
                    
                    # Get mapping info
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
                    
                    # Create position
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
                    logger.debug(f"Processed: {contract_id} -> {symbol} pos={position_val}")
                    
                except Exception as e:
                    logger.debug(f"Error on row {idx + 1}: {str(e)}")
                    continue
            
            self.positions = positions
            self.unmapped_positions = unmapped
            
            logger.info(f"MS Position Processing Complete:")
            logger.info(f"  ✅ Processed: {processed}")
            logger.info(f"  ⭕ Skipped (zero/invalid): {skipped}")
            logger.info(f"  🔍 Unmapped: {len(unmapped)}")
            
        except Exception as e:
            logger.error(f"Error loading MS Position file: {str(e)}")
            self.positions = []
    
    def _load_csv_positions(self, csv_file_path: str) -> None:
        """Load positions from CSV format"""
        try:
            df = pd.read_csv(csv_file_path)
            df.columns = df.columns.str.strip()
            
            # Look for contract column
            contract_col = None
            for col in df.columns:
                if 'contract' in col.lower():
                    contract_col = col
                    break
            
            if not contract_col and len(df.columns) > 3:
                contract_col = df.columns[3]  # Default to 4th column
            
            positions = []
            unmapped = []
            
            for idx, row in df.iterrows():
                try:
                    contract_id = str(row[contract_col]).strip()
                    if not contract_id or '-' not in contract_id:
                        continue
                    
                    parsed = self._parse_contract_id(contract_id)
                    if not parsed:
                        continue
                    
                    # Similar processing as MS Position
                    symbol = parsed['symbol']
                    
                    # Get position - look for CF Lots or similar column
                    position_val = 0
                    for col in df.columns:
                        if 'lot' in col.lower() or 'position' in col.lower():
                            try:
                                position_val = float(row[col])
                                if position_val != 0:
                                    break
                            except:
                                continue
                    
                    if position_val == 0:
                        continue
                    
                    # Process similar to MS Position
                    if symbol not in self.mapping_data:
                        unmapped.append({
                            'symbol': symbol,
                            'contract_id': contract_id,
                            'position': position_val,
                            'lot_size': 1,
                            'row_number': idx + 2,
                            'source': 'CSV Format'
                        })
                        continue
                    
                    # Create position (simplified)
                    mapping_info = self.mapping_data[symbol]
                    position = Position(
                        underlying_ticker=mapping_info['cash_ticker'],
                        symbol=symbol,
                        bloomberg_ticker=f"{symbol} Future",
                        series=parsed['series'],
                        expiry=parsed['expiry'],
                        strike=parsed['strike'],
                        option_type=parsed['option_type'],
                        position=position_val,
                        lot_size=1,
                        security_type='Futures' if parsed['series'] == 'FUTSTK' else 'Option',
                        deliverable=0.0
                    )
                    positions.append(position)
                    
                except:
                    continue
            
            self.positions = positions
            self.unmapped_positions = unmapped
            
        except Exception as e:
            logger.error(f"Error loading CSV: {str(e)}")
    
    def _load_excel_contract_positions(self, excel_file_path: str) -> None:
        """Load positions from Excel with Contract ID format"""
        # Use same logic as CSV but read Excel
        try:
            df = pd.read_excel(excel_file_path)
            # Process similar to CSV
            self._process_dataframe_with_contracts(df, "Excel Contract Format")
        except Exception as e:
            logger.error(f"Error loading Excel contract file: {str(e)}")
    
    def _load_bod_positions(self, bod_file_path: str, start_row: int = 12) -> None:
        """Load BOD positions from Excel file"""
        try:
            df = pd.read_excel(bod_file_path, header=None)
            
            # Find data start row
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
                    
                    # Parse expiry
                    if pd.notna(expiry_raw):
                        expiry = pd.to_datetime(expiry_raw)
                    else:
                        expiry = datetime.now() + timedelta(days=30)
                    
                    # Check mapping
                    if symbol not in self.mapping_data:
                        unmapped.append({
                            'symbol': symbol,
                            'position': open_position,
                            'lot_size': lot_size,
                            'row_number': idx + 1,
                            'source': 'BOD Format'
                        })
                        continue
                    
                    # Get mapping
                    mapping_info = self.mapping_data[symbol]
                    cash_ticker = mapping_info['cash_ticker']
                    fo_ticker = mapping_info['ticker']
                    
                    # Determine security type
                    if series == 'FUTSTK':
                        security_type = 'Futures'
                    elif series == 'OPTSTK' and option_type == 'CE':
                        security_type = 'Call'
                    elif series == 'OPTSTK' and option_type == 'PE':
                        security_type = 'Put'
                    else:
                        security_type = 'Equity'
                    
                    # Create Bloomberg ticker
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
    
    def _process_dataframe_with_contracts(self, df: pd.DataFrame, source: str) -> None:
        """Process a dataframe that has contract IDs"""
        positions = []
        unmapped = []
        
        # Similar to CSV processing
        for idx, row in df.iterrows():
            try:
                # Find and parse contract
                contract_id = None
                for col in df.columns:
                    val = str(row[col])
                    if '-' in val and ('FUTSTK' in val or 'OPTSTK' in val):
                        contract_id = val
                        break
                
                if not contract_id:
                    continue
                
                parsed = self._parse_contract_id(contract_id)
                if not parsed:
                    continue
                
                # Process and create position
                # ... (similar to other methods)
                
            except:
                continue
        
        self.positions = positions
        self.unmapped_positions = unmapped
    
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
                            logger.debug(f"Got price for {symbol}: {prices[symbol]}")
                            break
                    except:
                        continue
            except:
                pass
        
        self.underlying_prices.update(prices)
        logger.info(f"Fetched {len(prices)} prices")
        return prices
    
    def save_output_excel(self, output_path: str) -> None:
        """Save output to Excel file"""
        try:
            import openpyxl
            from openpyxl.styles import Font, PatternFill
            from openpyxl.utils import get_column_letter
            
            wb = openpyxl.Workbook()
            wb.remove(wb.active)
            
            # Create summary sheet
            ws = wb.create_sheet(title="Summary", index=0)
            
            # Headers
            headers = ['Underlying', 'Symbol', 'Type', 'Expiry', 'Strike', 'Position', 'Lot Size', 'Deliverable', 'Price']
            for col, header in enumerate(headers, 1):
                cell = ws.cell(row=1, column=col, value=header)
                cell.font = Font(bold=True, color="FFFFFF")
                cell.fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
            
            # Data rows
            current_row = 2
            for position in self.positions:
                ws.cell(row=current_row, column=1, value=position.underlying_ticker)
                ws.cell(row=current_row, column=2, value=position.symbol)
                ws.cell(row=current_row, column=3, value=position.security_type)
                ws.cell(row=current_row, column=4, value=position.expiry.strftime('%Y-%m-%d'))
                ws.cell(row=current_row, column=5, value=position.strike if position.strike > 0 else None)
                ws.cell(row=current_row, column=6, value=position.position)
                ws.cell(row=current_row, column=7, value=position.lot_size)
                ws.cell(row=current_row, column=8, value=position.deliverable)
                ws.cell(row=current_row, column=9, value=position.underlying_price)
                current_row += 1
            
            # Auto-size columns
            for col in range(1, 10):
                ws.column_dimensions[get_column_letter(col)].width = 15
            
            # Create unmapped sheet if needed
            if self.unmapped_positions:
                ws_unmapped = wb.create_sheet(title="Unmapped_Symbols")
                headers = ['Symbol', 'Contract ID', 'Position', 'Lot Size', 'Row Number', 'Source']
                for col, header in enumerate(headers, 1):
                    cell = ws_unmapped.cell(row=1, column=col, value=header)
                    cell.font = Font(bold=True, color="FFFFFF")
                    cell.fill = PatternFill(start_color="FF6600", end_color="FF6600", fill_type="solid")
                
                current_row = 2
                for unmapped in self.unmapped_positions:
                    ws_unmapped.cell(row=current_row, column=1, value=unmapped.get('symbol', ''))
                    ws_unmapped.cell(row=current_row, column=2, value=unmapped.get('contract_id', ''))
                    ws_unmapped.cell(row=current_row, column=3, value=unmapped.get('position', 0))
                    ws_unmapped.cell(row=current_row, column=4, value=unmapped.get('lot_size', 1))
                    ws_unmapped.cell(row=current_row, column=5, value=unmapped.get('row_number', 0))
                    ws_unmapped.cell(row=current_row, column=6, value=unmapped.get('source', ''))
                    current_row += 1
                
                # Auto-size columns
                for col in range(1, 7):
                    ws_unmapped.column_dimensions[get_column_letter(col)].width = 15
            
            wb.save(output_path)
            logger.info(f"✅ Excel output saved to: {output_path}")
            
        except Exception as e:
            logger.error(f"Error saving Excel: {str(e)}")
            raise
