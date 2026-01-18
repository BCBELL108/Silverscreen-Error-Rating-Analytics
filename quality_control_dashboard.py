import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

# ============================================================================
# DATABASE SETUP
# ============================================================================

DB_PATH = Path(__file__).parent / "quality_control.db"

def init_db():
    """Initialize SQLite database with jobs and customers tables"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Jobs table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER NOT NULL,
            job_number TEXT NOT NULL,
            date_entered TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            production_date DATE,
            total_pieces INTEGER NOT NULL,
            total_impressions INTEGER NOT NULL,
            total_damages INTEGER NOT NULL,
            error_rate REAL NOT NULL,
            notes TEXT,
            FOREIGN KEY (customer_id) REFERENCES customers (id)
        )
    """)
    
    # Customers table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_name TEXT UNIQUE NOT NULL,
            date_added TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            active INTEGER DEFAULT 1
        )
    """)
    
    conn.commit()
    conn.close()

def load_default_customers():
    """Load default customer list if database is empty"""
    default_customers = [
        "2469 - The UPS Store", "33.Black, LLC", "4M Promotions", "503 Network LLC", "714 Creative",
        "A4 Promotions", "Abacus Products, Inc.", "ACI Printing Services, Inc.", "Adaptive Branding",
        "Ad Stuff, Inc.", "Albrecht (Branding by Beth)", "Alpenglow Sports Inc", "AMB3R LLC",
        "American Solutions for Business", "Anning Johnson Company", "Aramark (Vestis)",
        "Armstrong Print & Promotional", "Badass Lass", "Bimark, Inc.", "Blackridge Branding",
        "Blue Label Distribution (HiLife)", "Bluelight Promotions", "BPL Supplies Inc",
        "Brand Original IPU", "Bravo Promotional Marketing", "Brent Binnall Enterprises",
        "Bright Print Works", "BSN Sports", "Bulldog Creative Agency", "B&W Wholesale",
        "Calla Products, LLC", "Care Youth Corporation", "Cariloha", "CDA Printing",
        "Classic Awards & Promotions", "Clayton AP Academy", "CLNC Sports dba Secondslide",
        "Clove and Twine", "Club Colors", "Clutch Creative", "Cole Apparel",
        "Color Graphics Screenprinting", "Colossal Printing Company LLC",
        "Cool Breeze Heating & Air Conditioning", "Corporate Couture",
        "Creative Marketing and Design AIA", "CrossFreedom", "Defero Swag", "Del Sol",
        "Deso Supply", "DFS West", "Divide Graphics", "Divot Dawgs", "Emblazeon",
        "eRetailing Associates, LLC", "Etched in Stone", "Eureka Shirt Circuit",
        "Evident Industries", "Factory Design Group", "Fastenal", "Feature Graphix",
        "Four Alarm Promotions IPU", "Four Twigs LLC", "Freedom USA (HiLife)", "Fuel",
        "GBrakes", "GeekHead Printing and Apparel", "Good News Collection",
        "Great Basin Decoration", "Gulf Coast Trades Center", "HALO/AdSource", "Happiscribble",
        "High Desert Print Company", "Home Means Nevada Co", "Hooked on Swag",
        "HSG Safety Supplies Inc.", "HSM Enterprises", "ICO Companies dba Red The Uniform Tailor",
        "Ideal Printing, Promos & Wearables", "Image Group", "Image Source",
        "Imagework Marketing", "Initial Impression", "Inkwell (Brandito)",
        "Innovative Impressions IPU", "Inproma LLC", "International Minute Press",
        "IZA Design Inc", "Jen McFerrin Creative", "Jetset Promotions LLC", "J&J Printing",
        "Johnson Promotions", "J&R Gear", "Kids Blanks", "Knoblauch Advertising",
        "Kug - Proforma", "Lakeview Threads", "Logo Boss", "Lookout Promotions",
        "LSK Branding", "Luxury Branded Goods", "Made to Order", "Madhouz LLC",
        "Makers NV", "Marco Ideas Unlimited", "Marco Polo Promotions LLC",
        "Matrix Promotional Marketing IPU", "Merch.com", "Monitor Premiums, LLC",
        "Montroy Signs & Graphics", "Moondeck", "Moore Promotions - Proforma",
        "Mountain Freak Boutique", "National Sports Apparel", "NDS AIA",
        "Needleworks Embroidery", "No Quarter Co", "North American Embroidery",
        "Northwood Creations", "Nothing Too Fancy", "On-Line Printing & Graphics",
        "Onyx Inc", "Opal Promotions", "Orangevale Copy Center", "Ozio Lifestyles LLC",
        "Paperworld Inc", "Par 5 Promotions", "Parle Enterprises, Inc",
        "Pica Marketing Group", "PIP Printing", "Premium Custom Solutions",
        "Print Head Inc", "Print Promo Factory", "Proforma Wine Country",
        "Proforma Your Best Corp.", "PromoCentric LLC", "Promo Dog Inc",
        "Promotional Edge", "Purpose-Built PRO", "Purpose-Built Retail", "Qhik Moto",
        "Quantum Graphics, Inc.", "Radar Promotions", "Rapt Clothing Inc",
        "Red Thread Labs", "Reno Motorsports Inc", "Reno Print Labs", "Reno Print Store",
        "Reno Typographers", "Rise Custom Apparel LLC", "Rite of Passage ATCS",
        "Rite of Passage Inc", "Rockland Aramark", "Round Up Creations LLC",
        "Rush Advertising LLC", "SanMar", "Score International", "SDG Promotions IPU",
        "Sierra Air", "Sierra Boat Company", "Sierra Mountain Graphics", "Signs by Van",
        "Silkletter", "Silkshop Screen Printing", "Silver Peak Promotions",
        "Silverscreen Decoration & Fulfillment", "Silverscreen Direct",
        "Skyward Corp dba Meridian Promotions", "SOBO Concepts LLC", "SpotFrog",
        "Spot On Signs", "Star Sports", "Sticker Pack", "Stock Roll Corp of America",
        "Swagger", "Swagoo Promotions", "Swizzle", "SynergyX1 LLC", "Tahoe Basics",
        "Tahoe LogoWear", "Teamworks", "Tee Shirt Bar", "The Graphics Factory",
        "The Hat Source", "The Right Promotions", "The Sourcing Group, LLC",
        "The Sourcing Group Promo", "Thunder House Productions LLC",
        "TPG Trade Show & Events", "Treasure Mountain", "Triangle Design & Graphics LLC",
        "TR Miller", "TRSTY Media", "Truly Gifted", "Tugboat, Inc",
        "University of Nevada Equipment Room", "Unraveled Threads", "Upper Park Clothing",
        "UP Shirt Inc", "Vail Dunlap", "Washoe County", "Washoe Schools",
        "Way to Be Designs, LLC", "WearyLand", "Windy City Promos", "Wolfgangs",
        "W&T Graphix", "Xcel", "YanceyWorks LLC", "Zazzle"
    ]
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Check if customers table is empty
    cursor.execute("SELECT COUNT(*) FROM customers")
    count = cursor.fetchone()[0]
    
    if count == 0:
        for customer in default_customers:
            cursor.execute("INSERT OR IGNORE INTO customers (customer_name) VALUES (?)", (customer,))
        conn.commit()
    
    conn.close()

def add_customer(customer_name):
    """Add a new customer"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        cursor.execute("INSERT INTO customers (customer_name) VALUES (?)", (customer_name,))
        conn.commit()
        conn.close()
        return True
    except sqlite3.IntegrityError:
        conn.close()
        return False

def get_all_customers():
    """Get all active customers"""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM customers WHERE active = 1 ORDER BY customer_name", conn)
    conn.close()
    return df

def add_job(customer_id, job_number, production_date, total_pieces, total_impressions, total_damages, notes=""):
    """Add a new job to the database"""
    error_rate = (total_damages / total_pieces * 100) if total_pieces > 0 else 0
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        INSERT INTO jobs (customer_id, job_number, production_date, total_pieces, total_impressions, total_damages, error_rate, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (customer_id, job_number, production_date, total_pieces, total_impressions, total_damages, error_rate, notes))
    
    conn.commit()
    conn.close()

def get_all_jobs():
    """Retrieve all jobs with customer names"""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("""
        SELECT j.*, c.customer_name 
        FROM jobs j
        JOIN customers c ON j.customer_id = c.id
        ORDER BY j.production_date DESC, j.date_entered DESC
    """, conn)
    conn.close()
    
    if not df.empty:
        df['production_date'] = pd.to_datetime(df['production_date'])
    
    return df

def get_jobs_by_customer(customer_id, start_date=None, end_date=None):
    """Get jobs for a specific customer, optionally filtered by date range"""
    conn = sqlite3.connect(DB_PATH)
    
    if start_date and end_date:
        df = pd.read_sql_query("""
            SELECT j.*, c.customer_name 
            FROM jobs j
            JOIN customers c ON j.customer_id = c.id
            WHERE j.customer_id = ? AND j.production_date BETWEEN ? AND ?
            ORDER BY j.production_date DESC
        """, conn, params=(customer_id, start_date, end_date))
    else:
        df = pd.read_sql_query("""
            SELECT j.*, c.customer_name 
            FROM jobs j
            JOIN customers c ON j.customer_id = c.id
            WHERE j.customer_id = ?
            ORDER BY j.production_date DESC
        """, conn, params=(customer_id,))
    
    conn.close()
    
    if not df.empty:
        df['production_date'] = pd.to_datetime(df['production_date'])
    
    return df

def get_jobs_by_date_range(start_date, end_date):
    """Get all jobs within a date range"""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("""
        SELECT j.*, c.customer_name 
        FROM jobs j
        JOIN customers c ON j.customer_id = c.id
        WHERE j.production_date BETWEEN ? AND ?
        ORDER BY j.production_date DESC
    """, conn, params=(start_date, end_date))
    conn.close()
    
    if not df.empty:
        df['production_date'] = pd.to_datetime(df['production_date'])
    
    return df

def get_customer_stats():
    """Get error rate statistics by customer"""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("""
        SELECT 
            c.customer_name,
            COUNT(j.id) as total_jobs,
            SUM(j.total_pieces) as total_pieces,
            SUM(j.total_damages) as total_damages,
            CASE 
                WHEN SUM(j.total_pieces) > 0 
                THEN (SUM(j.total_damages) * 100.0 / SUM(j.total_pieces))
                ELSE 0 
            END as error_rate
        FROM customers c
        LEFT JOIN jobs j ON c.id = j.customer_id
        WHERE c.active = 1
        GROUP BY c.id, c.customer_name
        HAVING total_jobs > 0
        ORDER BY error_rate DESC
    """, conn)
    conn.close()
    return df

def delete_job(job_id):
    """Delete a job"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
    conn.commit()
    conn.close()

# ============================================================================
# STREAMLIT APP
# ============================================================================

def main():
    st.set_page_config(
        page_title="Quality Control Dashboard",
        page_icon="📊",
        layout="wide"
    )
    
    # Initialize database and load default customers
    init_db()
    load_default_customers()
    
    # Header
    st.title("📊 Quality Control Dashboard")
    st.markdown("### SilverScreen Printing & Fulfillment")
    st.markdown("---")
    
    # Sidebar Navigation
    menu = st.sidebar.selectbox(
        "Navigation",
        ["📝 Enter Job Data", "📈 Customer Analytics", "🏢 All Customers Overview", "📋 View All Jobs", "👥 Manage Customers", "⚙️ Manage Data"]
    )
    
    # ========================================================================
    # DATA ENTRY PAGE
    # ========================================================================
    
    if menu == "📝 Enter Job Data":
        st.header("Enter New Job Data")
        
        # Customer selection
        customers_df = get_all_customers()
        customer_options = customers_df['customer_name'].tolist()
        
        selected_customer = st.selectbox(
            "Select Customer *",
            ["-- Select Customer --"] + customer_options,
            help="Choose the customer for this job"
        )
        
        if selected_customer == "-- Select Customer --":
            st.info("👆 Please select a customer to continue")
            return
        
        customer_id = customers_df[customers_df['customer_name'] == selected_customer]['id'].values[0]
        
        st.markdown("---")
        
        col1, col2 = st.columns(2)
        
        with col1:
            job_number = st.text_input("Job Number *", placeholder="e.g., FF-19547")
            production_date = st.date_input("Production Date *", value=datetime.today())
            total_pieces = st.number_input("Total Pieces Printed *", min_value=0, step=1)
            total_impressions = st.number_input("Total Impressions *", min_value=0, step=1)
        
        with col2:
            total_damages = st.number_input("Total Damages *", min_value=0, step=1)
            
            # Calculate error rate preview
            if total_pieces > 0:
                error_rate_preview = (total_damages / total_pieces) * 100
                st.metric("Error Rate Preview", f"{error_rate_preview:.2f}%")
            else:
                st.metric("Error Rate Preview", "0.00%")
            
            notes = st.text_area("Notes (Optional)", placeholder="Any additional notes about this job...")
        
        st.markdown("---")
        
        if st.button("💾 Save Job Data", type="primary", use_container_width=True):
            if not job_number:
                st.error("❌ Job Number is required!")
            elif total_pieces <= 0:
                st.error("❌ Total Pieces must be greater than 0!")
            else:
                add_job(customer_id, job_number, production_date, total_pieces, total_impressions, total_damages, notes)
                st.success(f"✅ Job {job_number} for {selected_customer} saved successfully!")
                st.balloons()
    
    # ========================================================================
    # CUSTOMER ANALYTICS
    # ========================================================================
    
    elif menu == "📈 Customer Analytics":
        st.header("Customer Analytics")
        
        # Customer selection
        customers_df = get_all_customers()
        customer_options = ["-- All Customers --"] + customers_df['customer_name'].tolist()
        
        selected_customer = st.selectbox("Select Customer", customer_options)
        
        # Date Range Filter
        col1, col2, col3 = st.columns([2, 2, 1])
        
        with col1:
            start_date = st.date_input("Start Date", value=datetime.today() - timedelta(days=30))
        
        with col2:
            end_date = st.date_input("End Date", value=datetime.today())
        
        with col3:
            if st.button("🔄 Refresh", use_container_width=True):
                st.rerun()
        
        # Get data
        if selected_customer == "-- All Customers --":
            df = get_jobs_by_date_range(start_date, end_date)
            st.subheader(f"All Customers - {start_date} to {end_date}")
        else:
            customer_id = customers_df[customers_df['customer_name'] == selected_customer]['id'].values[0]
            df = get_jobs_by_customer(customer_id, start_date, end_date)
            st.subheader(f"{selected_customer} - {start_date} to {end_date}")
        
        if df.empty:
            st.warning("📭 No jobs found for this selection.")
            return
        
        # Key Metrics
        st.markdown("### 📊 Key Metrics")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Jobs", len(df))
        
        with col2:
            total_pieces = df['total_pieces'].sum()
            st.metric("Total Pieces", f"{total_pieces:,}")
        
        with col3:
            total_damages = df['total_damages'].sum()
            st.metric("Total Damages", f"{total_damages:,}")
        
        with col4:
            overall_error_rate = (total_damages / total_pieces * 100) if total_pieces > 0 else 0
            st.metric("Error Rate", f"{overall_error_rate:.2f}%")
        
        st.markdown("---")
        
        # Charts
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### 📉 Error Rate Trend")
            fig = px.line(df, x='production_date', y='error_rate', markers=True, title="Error Rate Over Time")
            fig.update_layout(xaxis_title="Production Date", yaxis_title="Error Rate (%)", hovermode='x unified')
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("### Damages vs Total Pieces")
            fig = go.Figure()
            fig.add_trace(go.Bar(x=df['job_number'], y=df['total_pieces'], name='Total Pieces', marker_color='lightblue'))
            fig.add_trace(go.Bar(x=df['job_number'], y=df['total_damages'], name='Damages', marker_color='red'))
            fig.update_layout(title="Pieces vs Damages by Job", xaxis_title="Job Number", yaxis_title="Count", barmode='group')
            st.plotly_chart(fig, use_container_width=True)
        
        # Export
        st.markdown("---")
        csv = df.to_csv(index=False)
        st.download_button(
            label="📈 Download Report (CSV)",
            data=csv,
            file_name=f"qc_report_{selected_customer.replace(' ', '_')}_{start_date}_{end_date}.csv",
            mime="text/csv"
        )
    
    # ========================================================================
    # ALL CUSTOMERS OVERVIEW
    # ========================================================================
    
    elif menu == "🏢 All Customers Overview":
        st.header("All Customers Overview")
        
        stats_df = get_customer_stats()
        
        if stats_df.empty:
            st.info("📭 No job data available yet.")
            return
        
        # Overall Stats
        st.markdown("### Overall Statistics")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Customers", len(stats_df))
        
        with col2:
            st.metric("Total Jobs", int(stats_df['total_jobs'].sum()))
        
        with col3:
            st.metric("Total Pieces", f"{int(stats_df['total_pieces'].sum()):,}")
        
        with col4:
            overall_error = (stats_df['total_damages'].sum() / stats_df['total_pieces'].sum() * 100)
            st.metric("Company-Wide Error Rate", f"{overall_error:.2f}%")
        
        st.markdown("---")
        
        # Customer Comparison Charts
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### 🏆 Top 10 Best Customers (Lowest Error Rate)")
            best = stats_df.nsmallest(10, 'error_rate')
            fig = px.bar(best, x='customer_name', y='error_rate', title="Best Performing Customers")
            fig.update_layout(xaxis_title="Customer", yaxis_title="Error Rate (%)", showlegend=False)
            fig.update_xaxes(tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("### ⚠️ Top 10 Customers Needing Attention (Highest Error Rate)")
            worst = stats_df.nlargest(10, 'error_rate')
            fig = px.bar(worst, x='customer_name', y='error_rate', title="Customers Needing Attention", color_discrete_sequence=['red'])
            fig.update_layout(xaxis_title="Customer", yaxis_title="Error Rate (%)", showlegend=False)
            fig.update_xaxes(tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
        
        # Full Customer Table
        st.markdown("### 📋 All Customer Statistics")
        
        display_df = stats_df.copy()
        display_df['error_rate'] = display_df['error_rate'].apply(lambda x: f"{x:.2f}%")
        display_df['total_pieces'] = display_df['total_pieces'].apply(lambda x: f"{int(x):,}")
        display_df['total_damages'] = display_df['total_damages'].apply(lambda x: f"{int(x):,}")
        display_df['total_jobs'] = display_df['total_jobs'].apply(lambda x: int(x))
        
        st.dataframe(display_df, use_container_width=True, hide_index=True)
        
        # Export
        csv = stats_df.to_csv(index=False)
        st.download_button(
            label="📊 Download Customer Stats (CSV)",
            data=csv,
            file_name=f"customer_stats_{datetime.today().strftime('%Y-%m-%d')}.csv",
            mime="text/csv"
        )
    
    # ========================================================================
    # VIEW ALL JOBS
    # ========================================================================
    
    elif menu == "📋 View All Jobs":
        st.header("All Jobs")
        
        df = get_all_jobs()
        
        if df.empty:
            st.info("📭 No jobs in the database yet.")
            return
        
        st.markdown(f"### Total Jobs: {len(df)}")
        
        # Display data
        display_df = df.copy()
        display_df['error_rate'] = display_df['error_rate'].apply(lambda x: f"{x:.2f}%")
        display_df['production_date'] = display_df['production_date'].dt.strftime('%Y-%m-%d')
        
        st.dataframe(
            display_df[['customer_name', 'job_number', 'production_date', 'total_pieces', 'total_impressions', 'total_damages', 'error_rate', 'notes']],
            use_container_width=True,
            hide_index=True
        )
        
        # Search
        st.markdown("---")
        st.markdown("### 🔍 Search Jobs")
        
        col1, col2 = st.columns(2)
        
        with col1:
            search_term = st.text_input("Search by Job Number", placeholder="Enter job number...")
        
        with col2:
            customer_filter = st.selectbox("Filter by Customer", ["-- All --"] + df['customer_name'].unique().tolist())
        
        # Apply filters
        filtered = df.copy()
        
        if search_term:
            filtered = filtered[filtered['job_number'].str.contains(search_term, case=False, na=False)]
        
        if customer_filter != "-- All --":
            filtered = filtered[filtered['customer_name'] == customer_filter]
        
        if not filtered.empty and (search_term or customer_filter != "-- All --"):
            st.markdown(f"#### Found {len(filtered)} result(s)")
            display_filtered = filtered.copy()
            display_filtered['error_rate'] = display_filtered['error_rate'].apply(lambda x: f"{x:.2f}%")
            display_filtered['production_date'] = display_filtered['production_date'].dt.strftime('%Y-%m-%d')
            
            st.dataframe(
                display_filtered[['customer_name', 'job_number', 'production_date', 'total_pieces', 'total_impressions', 'total_damages', 'error_rate', 'notes']],
                use_container_width=True,
                hide_index=True
            )
    
    # ========================================================================
    # MANAGE CUSTOMERS
    # ========================================================================
    
    elif menu == "👥 Manage Customers":
        st.header("Manage Customers")
        
        tab1, tab2 = st.tabs(["➕ Add New Customer", "📋 View All Customers"])
        
        with tab1:
            st.markdown("### Add New Customer")
            
            new_customer_name = st.text_input("Customer Name", placeholder="Enter customer name...")
            
            if st.button("➕ Add Customer", type="primary", use_container_width=True):
                if not new_customer_name:
                    st.error("❌ Customer name cannot be empty!")
                else:
                    success = add_customer(new_customer_name.strip())
                    if success:
                        st.success(f"✅ Customer '{new_customer_name}' added successfully!")
                        st.balloons()
                    else:
                        st.error(f"❌ Customer '{new_customer_name}' already exists!")
        
        with tab2:
            st.markdown("### All Customers")
            
            customers_df = get_all_customers()
            st.markdown(f"**Total Customers:** {len(customers_df)}")
            
            st.dataframe(
                customers_df[['customer_name', 'date_added']],
                use_container_width=True,
                hide_index=True
            )
    
    # ========================================================================
    # MANAGE DATA
    # ========================================================================
    
    elif menu == "⚙️ Manage Data":
        st.header("Manage Data")
        
        df = get_all_jobs()
        
        if df.empty:
            st.info("📭 No jobs to manage yet.")
            return
        
        st.markdown("### 🗑️ Delete Job")
        st.warning("⚠️ Warning: Deleting a job is permanent!")
        
        job_options = df.apply(lambda row: f"{row['customer_name']} - {row['job_number']} - {row['production_date'].strftime('%Y-%m-%d')} (ID: {row['id']})", axis=1).tolist()
        
        selected_job = st.selectbox("Select Job to Delete", ["-- Select --"] + job_options)
        
        if selected_job != "-- Select --":
            job_id = int(selected_job.split("ID: ")[1].rstrip(")"))
            job_details = df[df['id'] == job_id].iloc[0]
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.write(f"**Customer:** {job_details['customer_name']}")
                st.write(f"**Job Number:** {job_details['job_number']}")
                st.write(f"**Date:** {job_details['production_date'].strftime('%Y-%m-%d')}")
            
            with col2:
                st.write(f"**Pieces:** {job_details['total_pieces']:,}")
                st.write(f"**Damages:** {job_details['total_damages']:,}")
                st.write(f"**Error Rate:** {job_details['error_rate']:.2f}%")
            
            if st.button("🗑️ Delete This Job", type="primary"):
                delete_job(job_id)
                st.success(f"✅ Job deleted successfully!")
                st.rerun()

if __name__ == "__main__":
    main()
