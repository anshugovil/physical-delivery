"""
Streamlit Web Interface for Portfolio Transformer
Fixed version with correct method name
"""

import streamlit as st
import pandas as pd
import os
import tempfile
from datetime import datetime
import traceback
import sys

# Import the transformer module
from portfolio_transformer_universal_v3 import EnhancedPortfolioTransformer

# Page config
st.set_page_config(
    page_title="Portfolio Transformer v1.0",
    page_icon="📊",
    layout="wide"
)

# Title and description
st.title("📊 Portfolio Transformer v1.0")
st.markdown("Transform multiple portfolio formats into standardized deliverable reports")

# Create columns for layout
col1, col2 = st.columns([1, 2])

with col1:
    st.header("⚙️ Configuration")
    
    # Fund selection
    fund_name = st.selectbox(
        "Select Fund:",
        ["Aurigin", "Wafra"]
    )
    
    # File upload section
    st.header("📁 Upload Files")
    
    # Position file upload
    position_file = st.file_uploader(
        "Position File (Excel/CSV)",
        type=['xlsx', 'xls', 'csv'],
        help="Upload your position file in any of the supported formats"
    )
    
    # Mapping file upload (optional)
    mapping_file = st.file_uploader(
        "Mapping File (Optional - uses default if not provided)",
        type=['csv'],
        help="Upload a custom mapping file or leave empty to use 'futures mapping.csv'"
    )
    
    # Process button
    if st.button("🚀 Process File", type="primary"):
        if position_file is not None:
            try:
                # Create progress indicators
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                # Save uploaded files temporarily
                with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(position_file.name)[1]) as tmp_file:
                    tmp_file.write(position_file.read())
                    position_file_path = tmp_file.name
                
                # Save mapping file if provided
                mapping_file_path = "futures mapping.csv"
                if mapping_file is not None:
                    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp_mapping:
                        tmp_mapping.write(mapping_file.read())
                        mapping_file_path = tmp_mapping.name
                
                # Initialize transformer
                status_text.text("Loading mapping data...")
                progress_bar.progress(20)
                
                transformer = EnhancedPortfolioTransformer(fund_name)
                
                # Load mapping
                if os.path.exists(mapping_file_path):
                    transformer.load_mapping_data(mapping_file_path)
                else:
                    st.warning("No mapping file found. Using empty mapping.")
                
                # Load positions
                status_text.text("Loading positions...")
                progress_bar.progress(40)
                transformer.load_positions(position_file_path)
                
                # Fetch prices and calculate
                status_text.text("Fetching prices and calculating deliverables...")
                progress_bar.progress(60)
                transformer.calculate_deliverables(auto_fetch_prices=True)
                
                # Generate output
                status_text.text("Generating Excel output...")
                progress_bar.progress(80)
                
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                output_file = f"{fund_name}_{timestamp}.xlsx"
                transformer.save_output_excel(output_file)
                
                # Get statistics - FIXED: use get_summary_stats (with underscore)
                stats = transformer.get_summary_stats()
                
                progress_bar.progress(100)
                status_text.text("✅ Processing complete!")
                
                # Store results in session state
                st.session_state['output_file'] = output_file
                st.session_state['stats'] = stats
                st.session_state['transformer'] = transformer
                
                # Clean up temp files
                os.unlink(position_file_path)
                if mapping_file is not None:
                    os.unlink(mapping_file_path)
                
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")
                st.code(traceback.format_exc())
        else:
            st.warning("Please upload a position file first!")

with col2:
    st.header("📊 Results")
    
    # Show results if available
    if 'stats' in st.session_state:
        stats = st.session_state['stats']
        
        # Summary metrics
        col_a, col_b, col_c, col_d = st.columns(4)
        
        with col_a:
            st.metric("Total Positions", stats['total_positions'])
        with col_b:
            st.metric("Total Underlyings", stats['total_underlyings'])
        with col_c:
            st.metric("Input Format", stats['input_format'])
        with col_d:
            st.metric("Unmapped Symbols", stats.get('unmapped_count', 0))
        
        # Positions by type
        if stats['positions_by_type']:
            st.subheader("Positions by Type")
            type_df = pd.DataFrame(
                list(stats['positions_by_type'].items()),
                columns=['Type', 'Count']
            )
            st.dataframe(type_df, use_container_width=True)
        
        # Unmapped symbols warning
        if stats.get('unmapped_count', 0) > 0:
            with st.expander(f"⚠️ {stats['unmapped_count']} Unmapped Positions", expanded=True):
                unmapped_symbols = stats.get('unmapped_symbols', [])
                if unmapped_symbols:
                    st.write("Symbols without mapping:")
                    # Display in columns for better layout
                    n_cols = 3
                    cols = st.columns(n_cols)
                    for i, symbol in enumerate(unmapped_symbols[:15]):
                        cols[i % n_cols].write(f"• {symbol}")
                    if len(unmapped_symbols) > 15:
                        st.write(f"... and {len(unmapped_symbols) - 15} more")
                    st.info("Check 'Unmapped_Symbols' sheet in the output file for details")
        
        # Sample positions
        if 'transformer' in st.session_state:
            transformer = st.session_state['transformer']
            if transformer.positions:
                st.subheader("Sample Processed Positions")
                sample_data = []
                for pos in transformer.positions[:5]:
                    sample_data.append({
                        'Symbol': pos.symbol,
                        'Type': pos.security_type,
                        'Position': pos.position,
                        'Lot Size': pos.lot_size,
                        'Expiry': pos.expiry.strftime('%Y-%m-%d'),
                        'Strike': pos.strike if pos.strike > 0 else '-',
                        'Deliverable': pos.deliverable
                    })
                sample_df = pd.DataFrame(sample_data)
                st.dataframe(sample_df, use_container_width=True)
        
        # Download button
        if 'output_file' in st.session_state and os.path.exists(st.session_state['output_file']):
            with open(st.session_state['output_file'], 'rb') as f:
                file_bytes = f.read()
                st.download_button(
                    label="📥 Download Excel Report",
                    data=file_bytes,
                    file_name=st.session_state['output_file'],
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    type="primary"
                )
    else:
        st.info("Upload a file and click 'Process File' to see results")

# Instructions section
with st.expander("📖 Instructions", expanded=False):
    st.markdown("""
    ### How to use:
    1. **Select Fund**: Choose between Aurigin and Wafra
    2. **Upload Position File**: Upload your position file in any of these formats:
       - BOD Excel format (Day Beginning positions)
       - CSV/Excel with Contract Id format
       - MS Position sheet format
    3. **Upload Mapping File** (Optional): If not provided, will use default 'futures mapping.csv'
    4. **Click Process**: The system will automatically:
       - Detect the input format
       - Load and process positions
       - Fetch Yahoo Finance prices
       - Calculate deliverables
       - Generate Excel report with multiple sheets
    
    ### Output includes:
    - Net Position Summary
    - Price Alerts for options
    - Master sheet with all positions
    - Individual expiry sheets
    - Unmapped symbols sheet (if any)
    - Collapsible grouped rows by underlying
    - Bloomberg formula integration
    """)

# Footer
st.markdown("---")
st.markdown("Portfolio Transformer v1.0 | Built with Streamlit")
