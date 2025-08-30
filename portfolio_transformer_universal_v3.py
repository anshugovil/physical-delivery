"""
Fix for consistent Excel row grouping across all input formats

The issue: Grouping works for BOD but not for other formats
Root cause: Data validation or structure differences preventing grouping
Solution: Enhanced grouping logic with better error handling
"""

def save_output_excel(self, output_path: str) -> None:
    """Save output to Excel file with consistent grouping for all formats"""
    try:
        import openpyxl
        from openpyxl.utils import get_column_letter
        from openpyxl.styles import Font, PatternFill
        
        # Debug logging
        logger.info(f"📊 Saving output: {len(self.positions)} mapped positions, {len(self.unmapped_positions)} unmapped positions")
        logger.info(f"📊 Input format detected: {self.input_format}")
        
        if not self.positions and not self.unmapped_positions:
            logger.warning("No positions to export")
            return
        
        # Create workbook and remove default sheet
        wb = openpyxl.Workbook()
        wb.remove(wb.active)
        
        # Create sheets for mapped positions if any exist
        if self.positions:
            # Create Net Position Summary sheet FIRST
            self._create_net_position_summary(wb, self.positions)
            logger.info(f"✅ Created Net Position Summary sheet")
            
            # Create Price Alert sheet
            self._create_price_alert_sheet(wb, self.positions)
            logger.info(f"✅ Created Price Alert sheet")
            
            # Group positions by expiry date
            expiry_groups = {}
            for position in self.positions:
                expiry_key = position.expiry.strftime('%Y-%m-%d')
                if expiry_key not in expiry_groups:
                    expiry_groups[expiry_key] = []
                expiry_groups[expiry_key].append(position)
            
            # Create master sheet with ALL positions - with enhanced grouping
            self._create_grouped_sheet_enhanced(wb, "Master_All_Expiries", self.positions)
            logger.info(f"✅ Created Master sheet with {len(self.positions)} positions")
            
            # Create individual expiry sheets
            for expiry_date, positions in sorted(expiry_groups.items()):
                sheet_name = f"Expiry_{expiry_date.replace('-', '_')}"
                if len(sheet_name) > 31:
                    sheet_name = f"Exp_{expiry_date.replace('-', '_')}"
                
                self._create_grouped_sheet_enhanced(wb, sheet_name, positions)
                logger.info(f"✅ Created sheet '{sheet_name}' with {len(positions)} positions")
        
        # Create unmapped symbols sheet if there are any
        if self.unmapped_positions:
            logger.info(f"⚠️ Creating unmapped sheet with {len(self.unmapped_positions)} positions")
            self._create_unmapped_sheet(wb)
            logger.info(f"✅ Created 'Unmapped_Symbols' sheet with {len(self.unmapped_positions)} positions")
        else:
            logger.info("ℹ️ No unmapped positions found - all symbols had mappings")
        
        # Save workbook
        wb.save(output_path)
        logger.info(f"💾 Excel output saved to: {output_path}")
        
    except Exception as e:
        logger.error(f"Error saving Excel file: {str(e)}")
        raise

def _create_grouped_sheet_enhanced(self, workbook, sheet_name: str, positions: List) -> None:
    """Enhanced sheet creation with robust grouping that works for all formats"""
    
    from openpyxl.styles import Font, PatternFill
    from openpyxl.utils import get_column_letter
    
    ws = workbook.create_sheet(title=sheet_name)
    
    # Headers with BBG Price and BBG Deliverable
    headers = [
        'Underlying', 'Symbol', 'Expiry', 'Position', 'Type', 'Strike', 
        'System Deliverable', 'Override Deliverable', 'System Price', 'Override Price', 
        'BBG Price', 'BBG Deliverable'
    ]
    
    for col, header in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col, value=header)
        cell.font = Font(bold=True, size=12, color="FFFFFF")
        cell.fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
    
    # Group positions by underlying ticker
    grouped_positions = {}
    for position in positions:
        underlying = position.underlying_ticker
        if underlying not in grouped_positions:
            grouped_positions[underlying] = []
        grouped_positions[underlying].append(position)
    
    current_row = 2
    group_ranges = []
    
    # Sort underlyings for consistent output
    for underlying_ticker in sorted(grouped_positions.keys()):
        positions_group = grouped_positions[underlying_ticker]
        underlying_row = current_row
        
        # Write underlying summary row
        ws.cell(row=underlying_row, column=1, value=underlying_ticker)
        
        # Get representative position for prices
        repr_position = positions_group[0]
        system_price = repr_position.underlying_price
        if system_price is not None:
            system_price = round(system_price, 1)
        ws.cell(row=underlying_row, column=9, value=system_price)  # System Price (Column I)
        
        # BBG Price formula
        bbg_formula = f'=@BDP(A{underlying_row},"PX_LAST")'
        ws.cell(row=underlying_row, column=11, value=bbg_formula)  # BBG Price (Column K)
        
        # Style underlying summary row
        for col in range(1, 13):  # 12 columns
            cell = ws.cell(row=underlying_row, column=col)
            cell.fill = PatternFill(start_color="B8CCE4", end_color="B8CCE4", fill_type="solid")
            cell.font = Font(bold=True, size=11)
        
        current_row += 1
        detail_start_row = current_row
        
        # Write individual positions (sorted by expiry for consistency)
        for position in sorted(positions_group, key=lambda x: (x.expiry, x.strike, x.option_type)):
            # Basic data
            ws.cell(row=current_row, column=2, value=position.bloomberg_ticker)
            ws.cell(row=current_row, column=3, value=position.expiry.strftime('%Y-%m-%d'))
            ws.cell(row=current_row, column=4, value=position.position)
            ws.cell(row=current_row, column=5, value=position.security_type)
            ws.cell(row=current_row, column=6, value=position.strike if position.strike > 0 else None)
            
            # Link prices to underlying row
            ws.cell(row=current_row, column=9, value=f"=I{underlying_row}")   # System Price
            ws.cell(row=current_row, column=10, value=f"=J{underlying_row}")  # Override Price
            ws.cell(row=current_row, column=11, value=f"=K{underlying_row}")  # BBG Price
            
            # DELIVERABLE FORMULAS
            system_formula = self._make_system_formula(current_row, underlying_row, position)
            override_formula = self._make_override_formula(current_row, underlying_row, position)
            bbg_formula = self._make_bbg_formula(current_row, underlying_row, position)
            
            ws.cell(row=current_row, column=7, value=system_formula)    # System Deliverable
            ws.cell(row=current_row, column=8, value=override_formula)  # Override Deliverable
            ws.cell(row=current_row, column=12, value=bbg_formula)      # BBG Deliverable
            
            current_row += 1
        
        detail_end_row = current_row - 1
        
        # Add total formulas to underlying row
        if detail_end_row >= detail_start_row:
            ws.cell(row=underlying_row, column=7, value=f"=SUM(G{detail_start_row}:G{detail_end_row})")   # System Total
            ws.cell(row=underlying_row, column=8, value=f"=SUM(H{detail_start_row}:H{detail_end_row})")   # Override Total
            ws.cell(row=underlying_row, column=12, value=f"=SUM(L{detail_start_row}:L{detail_end_row})")  # BBG Total
            
            # Store group range for later processing
            group_ranges.append((detail_start_row, detail_end_row))
        else:
            # Single position - no detail rows to sum
            ws.cell(row=underlying_row, column=7, value=0)
            ws.cell(row=underlying_row, column=8, value=0)
            ws.cell(row=underlying_row, column=12, value=0)
    
    # Apply grouping - Enhanced with better error handling
    if group_ranges:
        try:
            # CRITICAL: Set outline properties BEFORE creating any groups
            ws.sheet_properties.outlinePr.summaryBelow = False
            ws.sheet_properties.outlinePr.summaryRight = False
            
            # Debug logging
            logger.info(f"🔧 Applying grouping to {len(group_ranges)} underlying groups in sheet '{sheet_name}'")
            
            # Apply grouping to each range
            successful_groups = 0
            for start_row, end_row in group_ranges:
                try:
                    if end_row > start_row:
                        # Multiple rows - create a group
                        ws.row_dimensions.group(start_row, end_row, hidden=True, outline_level=1)
                        successful_groups += 1
                        logger.debug(f"  ✅ Grouped rows {start_row}-{end_row}")
                    elif end_row == start_row:
                        # Single row - just hide it
                        ws.row_dimensions[start_row].hidden = True
                        successful_groups += 1
                        logger.debug(f"  ✅ Hid single row {start_row}")
                except Exception as group_error:
                    logger.warning(f"  ⚠️ Could not group rows {start_row}-{end_row}: {str(group_error)}")
            
            logger.info(f"✅ Successfully created {successful_groups}/{len(group_ranges)} row groups in sheet '{sheet_name}'")
            
        except Exception as e:
            logger.warning(f"⚠️ Could not create row groups in sheet '{sheet_name}': {str(e)}")
            logger.warning(f"   Sheet will be created without grouping")
    else:
        logger.info(f"ℹ️ No groups to create in sheet '{sheet_name}' (single underlying or no data)")
    
    # Auto-size columns
    for col in range(1, 13):
        ws.column_dimensions[get_column_letter(col)].width = 18
    
    # Freeze panes at row 2 (below headers)
    ws.freeze_panes = ws['A2']
    
    logger.info(f"✅ Completed sheet '{sheet_name}' with {len(positions)} positions")

# Also update the original _create_grouped_sheet to use the enhanced version
def _create_grouped_sheet(self, workbook, sheet_name: str, positions: List) -> None:
    """Redirect to enhanced version for consistency"""
    return self._create_grouped_sheet_enhanced(workbook, sheet_name, positions)
