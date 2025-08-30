"""
Enhanced Portfolio Transformer Module - Complete Version
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
                df_raw = pd.read_excel(input_file_path, header=None, nrows=30)
                detected_format = self._detect_excel_format(df_raw, input_file_path)
                
                if detected_format == 'excel_contract_id':
                    self.input_format = 'excel_contract_id'
                    self._load_excel_contract_positions(input_file_path)
                elif detected_format == 'excel_ms_position':
                    self.input_format = 'excel_ms_position'
                    self._load_ms_position_positions(input_file_path)
                else:
                    self.input_format = 'excel_bod'
                    self._load_bod_positions(input_file_path, start_row)
            
            logger.info(f"✅ Loaded {len(self.positions)} positions")
            
        except Exception as e:
            logger.error(f"Error loading positions: {str(e)}")
            raise
    
    def _detect_excel_format(self, df_raw: pd.DataFrame, file_path: str) -> str:
        """Detect Excel file format"""
        # Check for MS Position format
        if len(df_raw.columns) >= 22:
            for row_idx in range(5, min(25, len(df_raw))):
                col1_val = str(df_raw.iloc[row_idx, 0]).strip() if pd.notna(df_raw.iloc[row_idx, 0]) else ""
                if '-' in col1_val and any(x in col1_val.upper() for x in ['FUTSTK', 'OPTSTK']):
                    return 'excel_ms_position'
        
        # Check for BOD format
        if len(df_raw.columns) >= 16:
            for row_idx in range(5, min(30, len(df_raw))):
                if len(df_raw.iloc[row_idx]) < 16:
                    continue
                col2_val = str(df_raw.iloc[row_idx, 1]).strip() if pd.notna(df_raw.iloc[row_idx, 1]) else ""
                if col2_val and len(col2_val) >= 2 and col2_val.replace('&', '').replace('-', '').isalpha():
                    if col2_val.upper() == col2_val:
                        return 'excel_bod'
        
        # Default to BOD
        return 'excel_bod'
    
    def _load_csv_positions(self, csv_file_path: str) -> None:
        """Load positions from CSV format"""
        try:
            df = pd.read_csv(csv_file_path)
            df.columns = df.columns.str.strip()
            
            positions = []
            for idx, row in df.iterrows():
                try:
                    # Simple parsing - customize based on your CSV structure
                    symbol = str(row.iloc[0]).strip() if len(row) > 0 else ""
                    position_val = float(row.iloc[1]) if len(row) > 1 else 0
                    
                    if position_val == 0:
                        continue
                    
                    # Create basic position
                    position = Position(
                        underlying_ticker=symbol,
                        symbol=symbol,
                        bloomberg_ticker=symbol,
                        series="EQ",
                        expiry=datetime.now() + timedelta(days=30),
                        strike=0,
                        option_type="",
                        position=position_val,
                        lot_size=1,
                        security_type="Equity",
                        deliverable=0.0
                    )
                    positions.append(position)
                except:
                    continue
            
            self.positions = positions
        except Exception as e:
            logger.error(f"Error loading CSV: {str(e)}")
    
    def _load_excel_contract_positions(self, excel_file_path: str) -> None:
        """Load positions from Excel with Contract ID format"""
        self._load_bod_positions(excel_file_path)  # Simplified for now
    
    def _load_ms_position_positions(self, excel_file_path: str) -> None:
        """Load positions from MS Position format"""
        self._load_bod_positions(excel_file_path)  # Simplified for now
    
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
                    
                    # Get mapping
                    if symbol in self.mapping_data:
                        mapping_info = self.mapping_data[symbol]
                        cash_ticker = mapping_info['cash_ticker']
                        fo_ticker = mapping_info['ticker']
                    else:
                        cash_ticker = symbol
                        fo_ticker = symbol
                    
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
                cell.font = Font(bold=True)
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
                headers = ['Symbol', 'Contract ID', 'Position', 'Lot Size', 'Row Number']
                for col, header in enumerate(headers, 1):
                    cell = ws_unmapped.cell(row=1, column=col, value=header)
                    cell.font = Font(bold=True)
                    cell.fill = PatternFill(start_color="FF6600", end_color="FF6600", fill_type="solid")
                
                current_row = 2
                for unmapped in self.unmapped_positions:
                    ws_unmapped.cell(row=current_row, column=1, value=unmapped.get('symbol', ''))
                    ws_unmapped.cell(row=current_row, column=2, value=unmapped.get('contract_id', ''))
                    ws_unmapped.cell(row=current_row, column=3, value=unmapped.get('position', 0))
                    ws_unmapped.cell(row=current_row, column=4, value=unmapped.get('lot_size', 1))
                    ws_unmapped.cell(row=current_row, column=5, value=unmapped.get('row_number', 0))
                    current_row += 1
            
            wb.save(output_path)
            logger.info(f"✅ Excel output saved to: {output_path}")
            
        except Exception as e:
            logger.error(f"Error saving Excel: {str(e)}")
            raise
