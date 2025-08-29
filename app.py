"""
Streamlit Web App for Portfolio Transformer
Just run: streamlit run app.py
"""

import streamlit as st
import pandas as pd
from datetime import datetime
import tempfile
import os

# Import your existing code (assuming it's in the same folder)
from portfolio_transformer_universal_v3 import EnhancedPortfolioTransformer

# Page config
st.set_page_config(
    page_title="Portfolio Transformer",
    page_icon="📊",
    layout="centered"
)

# Title
st.title("📊 Portfolio Transformer")
st.markdown("---")

# Fund selection
fund_name = st.selectbox("Select Fund:", ["Aurigin", "Wafra"])

# File uploaders
st.subheader("📁 Upload Files")
position_file = st.file_uploader(
    "Position File (Excel/CSV)", 
    type=['xlsx', 'xls', 'csv'],
    help="Upload your position data file"
)

mapping_file = st.file_uploader(
    "Mapping File (Optional - uses default if not provided)", 
    type=['csv'],
    help="Upload futures mapping.csv or leave empty to use default"
)

# Process button
if st.button("🚀 Process File", type="primary", disabled=not position_file):
    try:
        # Show progress
        with st.spinner("Processing... This may take a minute for price fetching"):
            
            # Save uploaded files temporarily
            with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(position_file.name)[1]) as tmp_position:
                tmp_position.write(position_file.getvalue())
                position_path = tmp_position.name
            
            # Handle mapping file
            if mapping_file:
                with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp_mapping:
                    tmp_mapping.write(mapping_file.getvalue())
                    mapping_path = tmp_mapping.name
            else:
                # Check if default mapping exists
                if os.path.exists("futures mapping.csv"):
                    mapping_path = "futures mapping.csv"
                else:
                    st.error("❌ No mapping file provided and default 'futures mapping.csv' not found!")
                    st.stop()
            
            # Create transformer instance
            transformer = EnhancedPortfolioTransformer(fund_name)
            
            # Load mapping
            with st.status("Loading mapping data..."):
                transformer.load_mapping_data(mapping_path)
                st.write(f"✅ Loaded {len(transformer.mapping_data)} symbol mappings")
            
            # Load positions
            with st.status("Loading positions..."):
                transformer.load_positions(position_path)
                st.write(f"✅ Detected format: {transformer.input_format}")
                st.write(f"✅ Loaded {len(transformer.positions)} positions")
            
            # Calculate deliverables
            with st.status("Fetching prices and calculating deliverables..."):
                transformer.calculate_deliverables(auto_fetch_prices=True)
                st.write("✅ Prices fetched and deliverables calculated")
            
            # Generate output
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_filename = f"{fund_name}_{timestamp}.xlsx"
            output_path = os.path.join(tempfile.gettempdir(), output_filename)
            
            transformer.save_output_excel(output_path)
            
            # Get statistics
            stats = transformer.get_summary_stats()
            
            # Show success message
            st.success("✅ Processing Complete!")
            
            # Display statistics
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Positions", stats['total_positions'])
            with col2:
                st.metric("Total Underlyings", stats['total_underlyings'])
            with col3:
                st.metric("Input Format", stats['input_format'])
            
            # Show positions by type
            if stats['positions_by_type']:
                st.subheader("Positions by Type")
                df_types = pd.DataFrame(
                    list(stats['positions_by_type'].items()),
                    columns=['Type', 'Count']
                )
                st.dataframe(df_types, use_container_width=True)
            
            # Download button
            with open(output_path, 'rb') as file:
                st.download_button(
                    label="📥 Download Excel Output",
                    data=file.read(),
                    file_name=output_filename,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            
            # Clean up temp files
            os.unlink(position_path)
            if mapping_file:
                os.unlink(mapping_path)
            
    except Exception as e:
        st.error(f"❌ Error: {str(e)}")
        st.exception(e)

# Instructions
with st.expander("📖 Instructions"):
    st.markdown("""
    ### How to use:
    1. Select your fund (Aurigin or Wafra)
    2. Upload your position file (Excel or CSV)
    3. Optionally upload a mapping file (or use default)
    4. Click 'Process File'
    5. Download the result
    
    ### Supported formats:
    - **Format 1:** BOD Excel (Day Beginning positions)
    - **Format 2:** CSV with Contract Id
    - **Format 3:** MS Position Excel
    
    ### Features:
    - Auto-detects file format
    - Fetches Yahoo Finance prices
    - Generates Bloomberg formulas
    - Creates grouped Excel output
    """)

# Footer
st.markdown("---")
st.markdown("Portfolio Transformer v1.0 | Built with Streamlit")