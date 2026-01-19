import streamlit as st
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go

# ============================================================================
# DATABASE SETUP - PostgreSQL (Neon)
# ============================================================================

def get_connection():
    """Create a fresh connection to PostgreSQL database using Streamlit secrets"""
    try:
        conn = psycopg2.connect(
            host=st.secrets["neon"]["host"],
            database=st.secrets["neon"]["database"],
            user=st.secrets["neon"]["user"],
            password=st.secrets["neon"]["password"],
            port=st.secrets["neon"].get("port", 5432),
            sslmode=st.secrets["neon"].get("sslmode", "require")
        )
        return conn
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        st.stop()


def init_db():
    """Initialize PostgreSQL database with jobs and customers tables"""
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Customers table
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS customers (
                id SERIAL PRIMARY KEY,
                customer_name TEXT UNIQUE NOT NULL,
                date_added TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                active INTEGER DEFAULT 1,
                target_error_rate REAL DEFAULT 2.0
            )
            """
        )

        # Jobs table
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id SERIAL PRIMARY KEY,
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
            """
        )

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        st.error(f"Failed to initialize database: {e}")
        st.stop()


def load_default_customers():
    """Load default customer list if database is empty"""
    default_customers = [
        "2469 - The UPS Store",
        "33.Black, LLC",
        "4M Promotions",
        "503 Network LLC",
        "714 Creative",
        "A4 Promotions",
        "Abacus Products, Inc.",
        "ACI Printing Services, Inc.",
        "Adaptive Branding",
        "Ad Stuff, Inc.",
        "Albrecht (Branding by Beth)",
        "Alpenglow Sports Inc",
        "AMB3R LLC",
        "American Solutions for Business",
        "Anning Johnson Company",
        "Aramark (Vestis)",
        "Armstrong Print & Promotional",
        "Badass Lass",
        "Bimark, Inc.",
        "Blackridge Branding",
        "Blue Label Distribution (HiLife)",
        "Bluelight Promotions",
        "BPL Supplies Inc",
        "Brand Original IPU",
        "Bravo Promotional Marketing",
        "Brent Binnall Enterprises",
        "Bright Print Works",
        "BSN Sports",
        "Bulldog Creative Agency",
        "B&W Wholesale",
        "Calla Products, LLC",
        "Care Youth Corporation",
        "Cariloha",
        "CDA Printing",
        "Classic Awards & Promotions",
        "Clayton AP Academy",
        "CLNC Sports dba Secondslide",
        "Clove and Twine",
        "Club Colors",
        "Clutch Creative",
        "Cole Apparel",
        "Color Graphics Screenprinting",
        "Colossal Printing Company LLC",
        "Cool Breeze Heating & Air Conditioning",
        "Corporate Couture",
        "Creative Marketing and Design AIA",
        "CrossFreedom",
        "Defero Swag",
        "Del Sol",
        "Deso Supply",
        "DFS West",
        "Divide Graphics",
        "Divot Dawgs",
        "Emblazeon",
        "eRetailing Associates, LLC",
        "Etched in Stone",
        "Eureka Shirt Circuit",
        "Evident Industries",
        "Factory Design Group",
        "Fastenal",
        "Feature Graphix",
        "Four Alarm Promotions IPU",
        "Four Twigs LLC",
        "Freedom USA (HiLife)",
        "Fuel",
        "GBrakes",
        "GeekHead Printing and Apparel",
        "Good News Collection",
        "Great Basin Decoration",
        "Gulf Coast Trades Center",
        "HALO/AdSource",
        "Happiscribble",
        "High Desert Print Company",
        "Home Means Nevada Co",
        "Hooked on Swag",
        "HSG Safety Supplies Inc.",
        "HSM Enterprises",
        "ICO Companies dba Red The Uniform Tailor",
        "Ideal Printing, Promos & Wearables",
        "Image Group",
        "Image Source",
        "Imagework Marketing",
        "Initial Impression",
        "Inkwell (Brandito)",
        "Innovative Impressions IPU",
        "Inproma LLC",
        "International Minute Press",
        "IZA Design Inc",
        "Jen McFerrin Creative",
        "Jetset Promotions LLC",
        "J&J Printing",
        "Johnson Promotions",
        "J&R Gear",
        "Kids Blanks",
        "Knoblauch Advertising",
        "Kug - Proforma",
        "Lakeview Threads",
        "Logo Boss",
        "Lookout Promotions",
        "LSK Branding",
        "Luxury Branded Goods",
        "Made to Order",
        "Madhouz LLC",
        "Makers NV",
        "Marco Ideas Unlimited",
        "Marco Polo Promotions LLC",
        "Matrix Promotional Marketing IPU",
        "Merch.com",
        "Monitor Premiums, LLC",
        "Montroy Signs & Graphics",
        "Moondeck",
        "Moore Promotions - Proforma",
        "Mountain Freak Boutique",
        "National Sports Apparel",
        "NDS AIA",
        "Needleworks Embroidery",
        "No Quarter Co",
        "North American Embroidery",
        "Northwood Creations",
        "Nothing Too Fancy",
        "On-Line Printing & Graphics",
        "Onyx Inc",
        "Opal Promotions",
        "Orangevale Copy Center",
        "Ozio Lifestyles LLC",
        "Paperworld Inc",
        "Par 5 Promotions",
        "Parle Enterprises, Inc",
        "Pica Marketing Group",
        "PIP Printing",
        "Premium Custom Solutions",
        "Print Head Inc",
        "Print Promo Factory",
        "Proforma Wine Country",
        "Proforma Your Best Corp.",
        "PromoCentric LLC",
        "Promo Dog Inc",
        "Promotional Edge",
        "Purpose-Built PRO",
        "Purpose-Built Retail",
        "Qhik Moto",
        "Quantum Graphics, Inc.",
        "Radar Promotions",
        "Rapt Clothing Inc",
        "Red Thread Labs",
        "Reno Motorsports Inc",
        "Reno Print Labs",
        "Reno Print Store",
        "Reno Typographers",
        "Rise Custom Apparel LLC",
        "Rite of Passage ATCS",
        "Rite of Passage Inc",
        "Rockland Aramark",
        "Round Up Creations LLC",
        "Rush Advertising LLC",
        "SanMar",
        "Score International",
        "SDG Promotions IPU",
        "Sierra Air",
        "Sierra Boat Company",
        "Sierra Mountain Graphics",
        "Signs by Van",
        "Silkletter",
        "Silkshop Screen Printing",
        "Silver Peak Promotions",
        "Silverscreen Decoration & Fulfillment",
        "Silverscreen Direct",
        "Skyward Corp dba Meridian Promotions",
        "SOBO Concepts LLC",
        "SpotFrog",
        "Spot On Signs",
        "Star Sports",
        "Sticker Pack",
        "Stock Roll Corp of America",
        "Swagger",
        "Swagoo Promotions",
        "Swizzle",
        "SynergyX1 LLC",
        "Tahoe Blue Sportswear",
        "Tamco Marketing Corporation",
        "Team Fitz Graphics, Inc.",
        "Team Sportswear",
        "Tenacious G LLC",
        "That's A Wrap Events IPU",
        "The Chappell Consulting Group",
        "The Geek",
        "The Image Boutique",
        "The Shirt Source",
        "Thread Logic",
        "Totally Promotional",
        "Traditions Screen Printing",
        "Twin Palm Promotional Products",
        "Tyler Wayne Designs",
        "Uniform Authority",
        "United Branding Consultants",
        "US Shirt",
        "Vagrant International Inc",
        "Vantage Apparel",
        "Virgin Valley Athletics",
        "Visualogic Proforma",
        "Wicked Printing",
        "Wolfpack",
        "Work Lab International",
        "Work Wearhouse, Inc.",
        "X-Grain Co",
        "Zee Medical",
    ]

    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Check if customers table is empty
        cursor.execute("SELECT COUNT(*) FROM customers")
        count = cursor.fetchone()[0]

        if count == 0:
            for customer in default_customers:
                try:
                    cursor.execute(
                        "INSERT INTO customers (customer_name) VALUES (%s)",
                        (customer,)
                    )
                    conn.commit()
                except psycopg2.IntegrityError:
                    conn.rollback()
                    continue

        cursor.close()
        conn.close()
    except Exception as e:
        st.error(f"Failed to load default customers: {e}")
        # Don't stop the app, just log the error


# ============================================================================
# DATABASE OPERATIONS
# ============================================================================

def add_customer(customer_name: str) -> bool:
    """Add a new customer to the database"""
    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute(
            "INSERT INTO customers (customer_name) VALUES (%s)",
            (customer_name,)
        )
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except psycopg2.IntegrityError:
        return False
    except Exception as e:
        st.error(f"Failed to add customer: {e}")
        return False


def get_all_customers() -> pd.DataFrame:
    """Retrieve all active customers"""
    conn = get_connection()
    df = pd.read_sql_query(
        """
        SELECT id, customer_name, date_added, target_error_rate
        FROM customers
        WHERE active = 1
        ORDER BY customer_name
        """,
        conn,
    )
    conn.close()
    return df


def update_customer_target(customer_id: int, target_rate: float) -> None:
    """Update target error rate for a customer"""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        "UPDATE customers SET target_error_rate = %s WHERE id = %s",
        (target_rate, customer_id),
    )

    conn.commit()
    cursor.close()
    conn.close()


def add_job(
    customer_id: int,
    job_number: str,
    production_date,
    total_pieces: int,
    total_impressions: int,
    total_damages: int,
    notes: str = "",
) -> None:
    """Add a new job to the database"""
    error_rate = (total_damages / total_pieces * 100) if total_pieces > 0 else 0

    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Convert date to string format if needed
        if hasattr(production_date, 'strftime'):
            production_date_str = production_date.strftime('%Y-%m-%d')
        else:
            production_date_str = str(production_date)

        cursor.execute(
            """
            INSERT INTO jobs (
                customer_id,
                job_number,
                production_date,
                total_pieces,
                total_impressions,
                total_damages,
                error_rate,
                notes
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                customer_id,
                job_number,
                production_date_str,
                total_pieces,
                total_impressions,
                total_damages,
                error_rate,
                notes,
            ),
        )

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        st.error(f"Failed to add job: {e}")
        raise


def get_all_jobs() -> pd.DataFrame:
    """Retrieve all jobs with customer names"""
    conn = get_connection()
    df = pd.read_sql_query(
        """
        SELECT j.*, c.customer_name
        FROM jobs j
        JOIN customers c ON j.customer_id = c.id
        ORDER BY j.production_date DESC, j.date_entered DESC
        """,
        conn,
    )
    conn.close()

    if not df.empty:
        df["production_date"] = pd.to_datetime(df["production_date"])

    return df


def get_jobs_by_customer(
    customer_id: int, start_date=None, end_date=None
) -> pd.DataFrame:
    """Get jobs for a specific customer, optionally filtered by date range"""
    conn = get_connection()

    if start_date and end_date:
        df = pd.read_sql_query(
            """
            SELECT j.*, c.customer_name
            FROM jobs j
            JOIN customers c ON j.customer_id = c.id
            WHERE j.customer_id = %s AND j.production_date BETWEEN %s AND %s
            ORDER BY j.production_date DESC
            """,
            conn,
            params=(customer_id, start_date, end_date),
        )
    else:
        df = pd.read_sql_query(
            """
            SELECT j.*, c.customer_name
            FROM jobs j
            JOIN customers c ON j.customer_id = c.id
            WHERE j.customer_id = %s
            ORDER BY j.production_date DESC
            """,
            conn,
            params=(customer_id,),
        )

    conn.close()

    if not df.empty:
        df["production_date"] = pd.to_datetime(df["production_date"])

    return df


def get_jobs_by_date_range(start_date, end_date) -> pd.DataFrame:
    """Get all jobs within a date range"""
    conn = get_connection()
    df = pd.read_sql_query(
        """
        SELECT j.*, c.customer_name
        FROM jobs j
        JOIN customers c ON j.customer_id = c.id
        WHERE j.production_date BETWEEN %s AND %s
        ORDER BY j.production_date DESC
        """,
        conn,
        params=(start_date, end_date),
    )
    conn.close()

    if not df.empty:
        df["production_date"] = pd.to_datetime(df["production_date"])

    return df


def get_customer_stats() -> pd.DataFrame:
    """Get error rate statistics by customer"""
    conn = get_connection()
    df = pd.read_sql_query(
        """
        SELECT
            c.customer_name,
            c.target_error_rate,
            COUNT(j.id) AS total_jobs,
            SUM(j.total_pieces) AS total_pieces,
            SUM(j.total_damages) AS total_damages,
            CASE
                WHEN SUM(j.total_pieces) > 0
                THEN (SUM(j.total_damages) * 100.0 / SUM(j.total_pieces))
                ELSE 0
            END AS error_rate
        FROM customers c
        LEFT JOIN jobs j ON c.id = j.customer_id
        WHERE c.active = 1
        GROUP BY c.id, c.customer_name, c.target_error_rate
        HAVING COUNT(j.id) > 0
        ORDER BY error_rate DESC
        """,
        conn,
    )
    conn.close()
    return df


def delete_job(job_id: int) -> None:
    """Delete a job"""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("DELETE FROM jobs WHERE id = %s", (job_id,))

    conn.commit()
    cursor.close()
    conn.close()


# ============================================================================
# STREAMLIT APP
# ============================================================================

def main():
    st.set_page_config(
        page_title="Screenprint QC Dashboard",
        page_icon="🎨",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # Initialize database and load default customers on first run
    init_db()
    load_default_customers()

    # ========================================================================
    # SIDEBAR & BRANDING
    # ========================================================================
    with st.sidebar:
        st.image(
            "https://www.silverscreenprinting.com/wp-content/uploads/2023/03/SilverScreen-Logo-Blue-White-Transparent-400x120.png",
            width=250,
        )
        st.markdown("---")

        st.title("Navigation")
        menu = st.radio(
            "Go to",
            [
                "📝 Job Data Submission",
                "📈 Customer Analytics",
                "🏢 All Customers Overview",
                "📋 View All Jobs",
                "👥 Manage Customers",
                "⚙️ Manage Data",
            ],
            label_visibility="collapsed",
        )

    st.title("Screenprint QC Dashboard")
    st.caption("Silverscreen Decoration & Fulfillment®")
    st.markdown("---")

    # ========================================================================
    # JOB DATA SUBMISSION PAGE (DEFAULT)
    # ========================================================================
    if menu == "📝 Job Data Submission":
        st.header("Job Data Submission")

        # Display success message if job was just saved
        if "job_saved" in st.session_state:
            st.success(st.session_state["job_saved"])
            del st.session_state["job_saved"]  # Clear the message

        customers_df = get_all_customers()
        customer_options = customers_df["customer_name"].tolist()
        
        # Simple selectbox for customer selection
        selected_customer = st.selectbox(
            "Select Customer *",
            ["-- Select Customer --"] + customer_options,
            help="Choose the customer for this job"
        )

        if selected_customer == "-- Select Customer --":
            st.info("👆 Please select a customer to continue")
            return

        customer_id = customers_df[
            customers_df["customer_name"] == selected_customer
        ]["id"].values[0]

        st.markdown("---")

        col1, col2 = st.columns(2)

        with col1:
            job_number = st.text_input("Job Number *", placeholder="e.g., FF-19547")
            production_date = st.date_input(
                "Production Date *", value=datetime.today()
            )
            total_pieces = st.number_input(
                "Total Pieces Printed *", min_value=0, step=1
            )
            total_impressions = st.number_input(
                "Total Impressions *", min_value=0, step=1
            )

        with col2:
            total_damages = st.number_input(
                "Total Damages *", min_value=0, step=1
            )

            if total_pieces > 0:
                error_rate_preview = (total_damages / total_pieces) * 100
                st.metric("Error Rate Preview", f"{error_rate_preview:.2f}%")
            else:
                st.metric("Error Rate Preview", "0.00%")

            notes = st.text_area(
                "Notes (Optional)",
                placeholder="Any additional notes about this job...",
            )

        st.markdown("---")

        if st.button("💾 Save Job Data", type="primary", use_container_width=True):
            if not job_number:
                st.error("❌ Job Number is required!")
            elif total_pieces <= 0:
                st.error("❌ Total Pieces must be greater than 0!")
            else:
                add_job(
                    customer_id,
                    job_number,
                    production_date,
                    total_pieces,
                    total_impressions,
                    total_damages,
                    notes,
                )
                # Store success message in session state
                st.session_state["job_saved"] = f"✅ Job {job_number} for {selected_customer} saved successfully!"
                st.rerun()

    # ========================================================================
    # CUSTOMER ANALYTICS
    # ========================================================================
    elif menu == "📈 Customer Analytics":
        st.header("Quality Control Metrics by Customer")

        customers_df = get_all_customers()
        customer_options = ["-- All Customers --"] + customers_df[
            "customer_name"
        ].tolist()

        selected_customer = st.selectbox("Select Customer", customer_options)

        col1, col2, col3 = st.columns([2, 2, 1])

        with col1:
            start_date = st.date_input(
                "Start Date", value=datetime.today() - timedelta(days=30)
            )

        with col2:
            end_date = st.date_input("End Date", value=datetime.today())

        with col3:
            if st.button("🔄 Refresh", use_container_width=True):
                st.rerun()

        if selected_customer == "-- All Customers --":
            df = get_jobs_by_date_range(start_date, end_date)
            st.subheader(f"All Customers - {start_date} to {end_date}")
            target_rate = 2.0
        else:
            customer_row = customers_df[
                customers_df["customer_name"] == selected_customer
            ].iloc[0]
            customer_id = customer_row["id"]
            target_rate = customer_row.get("target_error_rate", 2.0)
            if target_rate is None:
                target_rate = 2.0
            df = get_jobs_by_customer(customer_id, start_date, end_date)
            st.subheader(f"{selected_customer} - {start_date} to {end_date}")

        if df.empty:
            st.warning("📭 No jobs found for this selection.")
            return

        st.markdown("### 📊 Key Performance Metrics")

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Total Jobs", len(df))

        with col2:
            total_pieces = df["total_pieces"].sum()
            st.metric("Total Pieces", f"{total_pieces:,}")

        with col3:
            total_damages = df["total_damages"].sum()
            st.metric("Total Damages", f"{total_damages:,}")

        with col4:
            overall_error_rate = (
                (total_damages / total_pieces) * 100 if total_pieces > 0 else 0
            )
            st.metric("Overall Error Rate", f"{overall_error_rate:.2f}%")

        st.caption(f"Target error rate: {target_rate:.1f}%")

        st.markdown("---")

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("### 📉 Error Rate Trend")
            fig = px.line(
                df,
                x="production_date",
                y="error_rate",
                markers=True,
                title="Error Rate Over Time",
            )
            fig.update_layout(
                xaxis_title="Production Date",
                yaxis_title="Error Rate (%)",
                hovermode="x unified",
            )
            fig.add_hline(
                y=target_rate,
                line_dash="dash",
                annotation_text=f"Target {target_rate:.1f}%",
                annotation_position="top left",
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown("### 📊 Damages vs Total Pieces")
            fig = go.Figure()
            fig.add_trace(
                go.Bar(
                    x=df["job_number"],
                    y=df["total_pieces"],
                    name="Total Pieces",
                    marker_color="lightblue",
                )
            )
            fig.add_trace(
                go.Bar(
                    x=df["job_number"],
                    y=df["total_damages"],
                    name="Damages",
                    marker_color="red",
                )
            )
            fig.update_layout(
                xaxis_title="Job Number",
                yaxis_title="Count",
                barmode="overlay",
                hovermode="x unified",
            )
            st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")
        st.markdown("### 📋 Job Details")

        display_df = df.copy()
        display_df["error_rate"] = display_df["error_rate"].apply(
            lambda x: f"{x:.2f}%"
        )
        display_df["production_date"] = display_df["production_date"].dt.strftime(
            "%Y-%m-%d"
        )

        st.dataframe(
            display_df[
                [
                    "job_number",
                    "production_date",
                    "total_pieces",
                    "total_impressions",
                    "total_damages",
                    "error_rate",
                    "notes",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )

    # ========================================================================
    # ALL CUSTOMERS OVERVIEW
    # ========================================================================
    elif menu == "🏢 All Customers Overview":
        st.header("Customer Quality Overview")

        stats_df = get_customer_stats()

        if stats_df.empty:
            st.info("📭 No job data available yet.")
            return

        st.markdown("### 📊 Overall Quality Statistics")

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Total Customers", len(stats_df))

        with col2:
            st.metric("Total Jobs", int(stats_df["total_jobs"].sum()))

        with col3:
            st.metric(
                "Total Pieces",
                f"{int(stats_df['total_pieces'].sum()):,}",
            )

        with col4:
            overall_error = (
                stats_df["total_damages"].sum()
                / stats_df["total_pieces"].sum()
                * 100
            )
            st.metric("Company-Wide Error Rate", f"{overall_error:.2f}%")

        st.markdown("---")

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("### 🏆 Top 10 Best Customers (Lowest Error Rate)")
            best = stats_df.nsmallest(10, "error_rate")
            fig = px.bar(
                best,
                x="customer_name",
                y="error_rate",
                title="Best Performing Customers",
            )
            fig.update_layout(
                xaxis_title="Customer",
                yaxis_title="Error Rate (%)",
                showlegend=False,
            )
            fig.update_xaxes(tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown("### ⚠️ Top 10 Customers Needing Attention (Highest Error Rate)")
            worst = stats_df.nlargest(10, "error_rate")
            fig = px.bar(
                worst,
                x="customer_name",
                y="error_rate",
                title="Customers Needing Attention",
                color_discrete_sequence=["red"],
            )
            fig.update_layout(
                xaxis_title="Customer",
                yaxis_title="Error Rate (%)",
                showlegend=False,
            )
            fig.update_xaxes(tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")
        st.markdown("### 📋 All Customer Statistics")

        display_df = stats_df.copy()
        display_df["error_rate"] = display_df["error_rate"].apply(
            lambda x: f"{x:.2f}%"
        )
        display_df["target_error_rate"] = display_df["target_error_rate"].apply(
            lambda x: f"{x:.1f}%"
        )
        display_df["total_pieces"] = display_df["total_pieces"].apply(
            lambda x: f"{int(x):,}"
        )
        display_df["total_damages"] = display_df["total_damages"].apply(
            lambda x: f"{int(x):,}"
        )
        display_df["total_jobs"] = display_df["total_jobs"].apply(int)

        st.dataframe(display_df, use_container_width=True, hide_index=True)

        csv = stats_df.to_csv(index=False)
        st.download_button(
            label="📊 Download Customer Stats (CSV)",
            data=csv,
            file_name=f"customer_stats_{datetime.today().strftime('%Y-%m-%d')}.csv",
            mime="text/csv",
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

        display_df = df.copy()
        display_df["error_rate"] = display_df["error_rate"].apply(
            lambda x: f"{x:.2f}%"
        )
        display_df["production_date"] = display_df["production_date"].dt.strftime(
            "%Y-%m-%d"
        )

        st.dataframe(
            display_df[
                [
                    "customer_name",
                    "job_number",
                    "production_date",
                    "total_pieces",
                    "total_impressions",
                    "total_damages",
                    "error_rate",
                    "notes",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )

        st.markdown("---")
        st.markdown("### 🔍 Search Jobs")

        col1, col2 = st.columns(2)

        with col1:
            search_term = st.text_input(
                "Search by Job Number", placeholder="Enter job number..."
            )

        with col2:
            customer_filter = st.selectbox(
                "Filter by Customer",
                ["-- All --"] + df["customer_name"].unique().tolist(),
            )

        filtered = df.copy()

        if search_term:
            filtered = filtered[
                filtered["job_number"].str.contains(
                    search_term, case=False, na=False
                )
            ]

        if customer_filter != "-- All --":
            filtered = filtered[filtered["customer_name"] == customer_filter]

        if not filtered.empty and (search_term or customer_filter != "-- All --"):
            st.markdown(f"#### Found {len(filtered)} result(s)")
            display_filtered = filtered.copy()
            display_filtered["error_rate"] = display_filtered["error_rate"].apply(
                lambda x: f"{x:.2f}%"
            )
            display_filtered["production_date"] = display_filtered[
                "production_date"
            ].dt.strftime("%Y-%m-%d")

            st.dataframe(
                display_filtered[
                    [
                        "customer_name",
                        "job_number",
                        "production_date",
                        "total_pieces",
                        "total_impressions",
                        "total_damages",
                        "error_rate",
                        "notes",
                    ]
                ],
                use_container_width=True,
                hide_index=True,
            )

    # ========================================================================
    # MANAGE CUSTOMERS
    # ========================================================================
    elif menu == "👥 Manage Customers":
        st.header("Manage Customers")

        tab1, tab2 = st.tabs(["➕ Add New Customer", "📋 View & Set Targets"])

        with tab1:
            st.markdown("### Add New Customer")

            new_customer_name = st.text_input(
                "Customer Name", placeholder="Enter customer name..."
            )

            if st.button("➕ Add Customer", type="primary", use_container_width=True):
                if not new_customer_name:
                    st.error("❌ Customer name cannot be empty!")
                else:
                    success = add_customer(new_customer_name.strip())
                    if success:
                        st.success(
                            f"✅ Customer '{new_customer_name}' added successfully!"
                        )
                    else:
                        st.error(
                            f"❌ Customer '{new_customer_name}' already exists!"
                        )

        with tab2:
            st.markdown("### Customer List & Target Error Rates")

            customers_df = get_all_customers()
            st.markdown(f"**Total Customers:** {len(customers_df)}")

            display_df = customers_df.copy()
            display_df["target_error_rate"] = display_df[
                "target_error_rate"
            ].apply(lambda x: f"{x:.1f}%")

            st.dataframe(
                display_df[["customer_name", "date_added", "target_error_rate"]],
                use_container_width=True,
                hide_index=True,
            )

            st.markdown("---")
            st.markdown("### Set Target Error Rate by Customer")

            if not customers_df.empty:
                cust_name_for_target = st.selectbox(
                    "Select Customer",
                    customers_df["customer_name"].tolist(),
                )
                target_choice = st.selectbox(
                    "Target Error Rate",
                    ["3.0%", "2.0%", "1.0%"],
                    help="Standard quality targets. 1.0% is the most strict.",
                )
                target_value = float(target_choice.replace("%", ""))

                if st.button(
                    "💾 Update Target Error Rate", type="primary"
                ):
                    cust_id = customers_df[
                        customers_df["customer_name"] == cust_name_for_target
                    ]["id"].values[0]
                    update_customer_target(cust_id, target_value)
                    st.success(
                        f"✅ Updated target error rate for {cust_name_for_target} to {target_value:.1f}%"
                    )
                    st.rerun()

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

        job_options = df.apply(
            lambda row: f"{row['customer_name']} - {row['job_number']} - {row['production_date'].strftime('%Y-%m-%d')} (ID: {row['id']})",
            axis=1,
        ).tolist()

        selected_job = st.selectbox(
            "Select Job to Delete", ["-- Select --"] + job_options
        )

        if selected_job != "-- Select --":
            job_id = int(selected_job.split("ID: ")[1].rstrip(")"))
            job_details = df[df["id"] == job_id].iloc[0]

            col1, col2 = st.columns(2)

            with col1:
                st.write(f"**Customer:** {job_details['customer_name']}")
                st.write(f"**Job Number:** {job_details['job_number']}")
                st.write(
                    f"**Date:** {job_details['production_date'].strftime('%Y-%m-%d')}"
                )

            with col2:
                st.write(f"**Pieces:** {job_details['total_pieces']:,}")
                st.write(f"**Damages:** {job_details['total_damages']:,}")
                st.write(f"**Error Rate:** {job_details['error_rate']:.2f}%")

            if st.button("🗑️ Delete This Job", type="primary"):
                delete_job(job_id)
                st.success("✅ Job deleted successfully!")
                st.rerun()


if __name__ == "__main__":
    main()
