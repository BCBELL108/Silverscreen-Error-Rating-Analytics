import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy.exc import IntegrityError

# ============================================================================
# DATABASE (NEON / POSTGRES via Streamlit Secrets)
# ============================================================================

@st.cache_resource
def get_engine():
    # Uses Streamlit Cloud Secrets:
    # [connections.qc]
    # url="postgresql://...."
    return st.connection("qc", type="sql").engine


def init_db():
    """Initialize Postgres tables"""
    eng = get_engine()
    with eng.begin() as conn:
        # Customers table
        conn.exec_driver_sql(
            """
            CREATE TABLE IF NOT EXISTS customers (
                id SERIAL PRIMARY KEY,
                customer_name TEXT UNIQUE NOT NULL,
                date_added TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                active INTEGER DEFAULT 1,
                target_error_rate REAL DEFAULT 2.0
            );
            """
        )

        # Jobs table
        conn.exec_driver_sql(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id SERIAL PRIMARY KEY,
                customer_id INTEGER NOT NULL REFERENCES customers(id),
                job_number TEXT NOT NULL,
                date_entered TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                production_date DATE,
                total_pieces INTEGER NOT NULL,
                total_impressions INTEGER NOT NULL,
                total_damages INTEGER NOT NULL,
                error_rate REAL NOT NULL,
                notes TEXT
            );
            """
        )


def load_default_customers():
    """Load default customer list if customers table is empty"""
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
        "Tahoe Basics",
        "Tahoe LogoWear",
        "Teamworks",
        "Tee Shirt Bar",
        "The Graphics Factory",
        "The Hat Source",
        "The Right Promotions",
        "The Sourcing Group, LLC",
        "The Sourcing Group Promo",
        "Thunder House Productions LLC",
        "TPG Trade Show & Events",
        "Treasure Mountain",
        "Triangle Design & Graphics LLC",
        "TR Miller",
        "TRSTY Media",
        "Truly Gifted",
        "Tugboat, Inc",
        "University of Nevada Equipment Room",
        "Unraveled Threads",
        "Upper Park Clothing",
        "UP Shirt Inc",
        "Vail Dunlap",
        "Washoe County",
        "Washoe Schools",
        "Way to Be Designs, LLC",
        "WearyLand",
        "Windy City Promos",
        "Wolfgangs",
        "W&T Graphix",
        "Xcel",
        "YanceyWorks LLC",
        "Zazzle",
    ]

    eng = get_engine()
    with eng.begin() as conn:
        count = conn.exec_driver_sql("SELECT COUNT(*) FROM customers").scalar_one()

        if count == 0:
            for customer in default_customers:
                conn.exec_driver_sql(
                    """
                    INSERT INTO customers (customer_name)
                    VALUES (%s)
                    ON CONFLICT (customer_name) DO NOTHING
                    """,
                    (customer,),
                )


def add_customer(customer_name: str) -> bool:
    eng = get_engine()
    try:
        with eng.begin() as conn:
            conn.exec_driver_sql(
                "INSERT INTO customers (customer_name) VALUES (%s)",
                (customer_name,),
            )
        return True
    except IntegrityError:
        return False


def update_customer_target(customer_id: int, target_error_rate: float) -> None:
    eng = get_engine()
    with eng.begin() as conn:
        conn.exec_driver_sql(
            "UPDATE customers SET target_error_rate = %s WHERE id = %s",
            (target_error_rate, customer_id),
        )


def get_all_customers() -> pd.DataFrame:
    eng = get_engine()
    with eng.connect() as conn:
        return pd.read_sql(
            """
            SELECT id, customer_name, date_added, active, target_error_rate
            FROM customers
            WHERE active = 1
            ORDER BY customer_name
            """,
            conn,
        )


def add_job(
    customer_id: int,
    job_number: str,
    production_date,
    total_pieces: int,
    total_impressions: int,
    total_damages: int,
    notes: str = "",
) -> None:
    error_rate = (total_damages / total_pieces * 100) if total_pieces > 0 else 0

    eng = get_engine()
    with eng.begin() as conn:
        conn.exec_driver_sql(
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
                production_date,
                total_pieces,
                total_impressions,
                total_damages,
                error_rate,
                notes,
            ),
        )


def get_all_jobs() -> pd.DataFrame:
    eng = get_engine()
    with eng.connect() as conn:
        df = pd.read_sql(
            """
            SELECT j.*, c.customer_name
            FROM jobs j
            JOIN customers c ON j.customer_id = c.id
            ORDER BY j.production_date DESC, j.date_entered DESC
            """,
            conn,
        )

    if not df.empty and "production_date" in df.columns:
        df["production_date"] = pd.to_datetime(df["production_date"])

    return df


def get_jobs_by_customer(customer_id: int, start_date=None, end_date=None) -> pd.DataFrame:
    eng = get_engine()
    with eng.connect() as conn:
        if start_date and end_date:
            df = pd.read_sql(
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
            df = pd.read_sql(
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

    if not df.empty and "production_date" in df.columns:
        df["production_date"] = pd.to_datetime(df["production_date"])

    return df


def get_jobs_by_date_range(start_date, end_date) -> pd.DataFrame:
    eng = get_engine()
    with eng.connect() as conn:
        df = pd.read_sql(
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

    if not df.empty and "production_date" in df.columns:
        df["production_date"] = pd.to_datetime(df["production_date"])

    return df


def get_customer_stats() -> pd.DataFrame:
    eng = get_engine()
    with eng.connect() as conn:
        return pd.read_sql(
            """
            SELECT
                c.customer_name,
                c.target_error_rate,
                COUNT(j.id) AS total_jobs,
                COALESCE(SUM(j.total_pieces), 0) AS total_pieces,
                COALESCE(SUM(j.total_damages), 0) AS total_damages,
                CASE
                    WHEN COALESCE(SUM(j.total_pieces), 0) > 0
                    THEN (COALESCE(SUM(j.total_damages), 0) * 100.0 / COALESCE(SUM(j.total_pieces), 0))
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


def delete_job(job_id: int) -> None:
    eng = get_engine()
    with eng.begin() as conn:
        conn.exec_driver_sql("DELETE FROM jobs WHERE id = %s", (job_id,))


# ============================================================================
# STREAMLIT APP
# ============================================================================

def main():
    st.set_page_config(
        page_title="Screenprint QC Dashboard",
        page_icon="📊",
        layout="wide",
    )

    # Optional: quick connection check in sidebar
    try:
        eng = get_engine()
        with eng.connect() as conn:
            conn.exec_driver_sql("SELECT 1")
        # st.sidebar.success("✅ Database connected")
    except Exception as e:
        st.sidebar.error("❌ Database NOT connected")
        st.sidebar.exception(e)
        return

    init_db()
    load_default_customers()

    with st.sidebar:
        try:
            st.image("silverscreen_logo.png", use_column_width=True)
        except Exception:
            st.markdown("### 🎨 SilverScreen")

        st.markdown("---")

        st.markdown("### Navigation")
        menu = st.radio(
            "",
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

    st.markdown(
        "<h1 style='text-align:center;'>Screenprint QC Dashboard</h1>",
        unsafe_allow_html=True,
    )
    st.markdown(
        "<p style='text-align:center; font-size:14px; color:gray;'>Silverscreen Decoration & Fulfillment®</p>",
        unsafe_allow_html=True,
    )
    st.markdown("---")

    # ========================================================================
    # JOB DATA SUBMISSION PAGE (DEFAULT)
    # ========================================================================
    if menu == "📝 Job Data Submission":
        st.header("Job Data Submission")

        if "job_saved" in st.session_state:
            st.success(st.session_state["job_saved"])
            del st.session_state["job_saved"]

        customers_df = get_all_customers()
        customer_options = customers_df["customer_name"].tolist()

        selected_customer = st.selectbox(
            "Select Customer *",
            ["-- Select Customer --"] + customer_options,
            help="Choose the customer for this job",
        )

        if selected_customer == "-- Select Customer --":
            st.info("👆 Please select a customer to continue")
            return

        customer_id = customers_df[customers_df["customer_name"] == selected_customer]["id"].values[0]

        st.markdown("---")
        col1, col2 = st.columns(2)

        with col1:
            job_number = st.text_input("Job Number *", placeholder="e.g., FF-19547")
            production_date = st.date_input("Production Date *", value=datetime.today())
            total_pieces = st.number_input("Total Pieces Printed *", min_value=0, step=1)
            total_impressions = st.number_input("Total Impressions *", min_value=0, step=1)

        with col2:
            total_damages = st.number_input("Total Damages *", min_value=0, step=1)

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
                add_job(
                    customer_id,
                    job_number,
                    production_date,
                    total_pieces,
                    total_impressions,
                    total_damages,
                    notes,
                )
                st.session_state["job_saved"] = f"✅ Job {job_number} for {selected_customer} saved successfully!"
                st.rerun()

    # ========================================================================
    # CUSTOMER ANALYTICS
    # ========================================================================
    elif menu == "📈 Customer Analytics":
        st.header("Quality Control Metrics by Customer")

        customers_df = get_all_customers()
        customer_options = ["-- All Customers --"] + customers_df["customer_name"].tolist()
        selected_customer = st.selectbox("Select Customer", customer_options)

        col1, col2, col3 = st.columns([2, 2, 1])
        with col1:
            start_date = st.date_input("Start Date", value=datetime.today() - timedelta(days=30))
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
            customer_row = customers_df[customers_df["customer_name"] == selected_customer].iloc[0]
            customer_id = customer_row["id"]
            target_rate = customer_row.get("target_error_rate", 2.0) or 2.0
            df = get_jobs_by_customer(customer_id, start_date, end_date)
            st.subheader(f"{selected_customer} - {start_date} to {end_date}")

        if df.empty:
            st.warning("📭 No jobs found for this selection.")
            return

        st.markdown("### 📊 Key Performance Metrics")
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.metric("Total Jobs", len(df))
        with c2:
            total_pieces = int(df["total_pieces"].sum())
            st.metric("Total Pieces", f"{total_pieces:,}")
        with c3:
            total_damages = int(df["total_damages"].sum())
            st.metric("Total Damages", f"{total_damages:,}")
        with c4:
            overall_error_rate = (total_damages / total_pieces) * 100 if total_pieces > 0 else 0
            st.metric("Overall Error Rate", f"{overall_error_rate:.2f}%")

        st.caption(f"Target error rate: {target_rate:.1f}%")
        st.markdown("---")

        c1, c2 = st.columns(2)
        with c1:
            st.markdown("### 📉 Error Rate Trend")
            fig = px.line(df, x="production_date", y="error_rate", markers=True, title="Error Rate Over Time")
            fig.update_layout(xaxis_title="Production Date", yaxis_title="Error Rate (%)", hovermode="x unified")
            fig.add_hline(y=target_rate, line_dash="dash",
                          annotation_text=f"Target {target_rate:.1f}%",
                          annotation_position="top left")
            st.plotly_chart(fig, use_container_width=True)

        with c2:
            st.markdown("### 📊 Damages vs Total Pieces")
            fig = go.Figure()
            fig.add_trace(go.Bar(x=df["job_number"], y=df["total_pieces"], name="Total Pieces"))
            fig.add_trace(go.Bar(x=df["job_number"], y=df["total_damages"], name="Damages"))
            fig.update_layout(title="Pieces vs Damages by Job", xaxis_title="Job Number", yaxis_title="Count", barmode="group")
            st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")
        csv = df.to_csv(index=False)

        safe_name = "all_customers" if selected_customer == "-- All Customers --" else selected_customer.replace(" ", "_")
        st.download_button(
            label="📊 Download Report (CSV)",
            data=csv,
            file_name=f"qc_report_{safe_name}_{start_date}_{end_date}.csv",
            mime="text/csv",
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
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.metric("Total Customers", len(stats_df))
        with c2:
            st.metric("Total Jobs", int(stats_df["total_jobs"].sum()))
        with c3:
            st.metric("Total Pieces", f"{int(stats_df['total_pieces'].sum()):,}")
        with c4:
            overall_error = (stats_df["total_damages"].sum() / stats_df["total_pieces"].sum() * 100) if stats_df["total_pieces"].sum() else 0
            st.metric("Company-Wide Error Rate", f"{overall_error:.2f}%")

        st.markdown("---")
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("### 🏆 Top 10 Best Customers (Lowest Error Rate)")
            best = stats_df.nsmallest(10, "error_rate")
            fig = px.bar(best, x="customer_name", y="error_rate", title="Best Performing Customers")
            fig.update_layout(xaxis_title="Customer", yaxis_title="Error Rate (%)", showlegend=False)
            fig.update_xaxes(tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)

        with c2:
            st.markdown("### ⚠️ Top 10 Customers Needing Attention (Highest Error Rate)")
            worst = stats_df.nlargest(10, "error_rate")
            fig = px.bar(worst, x="customer_name", y="error_rate", title="Customers Needing Attention")
            fig.update_layout(xaxis_title="Customer", yaxis_title="Error Rate (%)", showlegend=False)
            fig.update_xaxes(tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)

        st.markdown("### 📋 All Customer Statistics")
        display_df = stats_df.copy()
        display_df["error_rate"] = display_df["error_rate"].apply(lambda x: f"{x:.2f}%")
        display_df["target_error_rate"] = display_df["target_error_rate"].apply(lambda x: f"{x:.1f}%")
        display_df["total_pieces"] = display_df["total_pieces"].apply(lambda x: f"{int(x):,}")
        display_df["total_damages"] = display_df["total_damages"].apply(lambda x: f"{int(x):,}")
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
        display_df["error_rate"] = display_df["error_rate"].apply(lambda x: f"{x:.2f}%")
        display_df["production_date"] = pd.to_datetime(display_df["production_date"]).dt.strftime("%Y-%m-%d")

        st.dataframe(
            display_df[
                ["customer_name", "job_number", "production_date", "total_pieces",
                 "total_impressions", "total_damages", "error_rate", "notes"]
            ],
            use_container_width=True,
            hide_index=True,
        )

        st.markdown("---")
        st.markdown("### 🔍 Search Jobs")
        c1, c2 = st.columns(2)
        with c1:
            search_term = st.text_input("Search by Job Number", placeholder="Enter job number...")
        with c2:
            customer_filter = st.selectbox("Filter by Customer", ["-- All --"] + df["customer_name"].unique().tolist())

        filtered = df.copy()
        if search_term:
            filtered = filtered[filtered["job_number"].str.contains(search_term, case=False, na=False)]
        if customer_filter != "-- All --":
            filtered = filtered[filtered["customer_name"] == customer_filter]

        if not filtered.empty and (search_term or customer_filter != "-- All --"):
            st.markdown(f"#### Found {len(filtered)} result(s)")
            display_filtered = filtered.copy()
            display_filtered["error_rate"] = display_filtered["error_rate"].apply(lambda x: f"{x:.2f}%")
            display_filtered["production_date"] = pd.to_datetime(display_filtered["production_date"]).dt.strftime("%Y-%m-%d")

            st.dataframe(
                display_filtered[
                    ["customer_name", "job_number", "production_date", "total_pieces",
                     "total_impressions", "total_damages", "error_rate", "notes"]
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
            st.markdown("### Customer List & Target Error Rates")
            customers_df = get_all_customers()
            st.markdown(f"**Total Customers:** {len(customers_df)}")

            display_df = customers_df.copy()
            display_df["target_error_rate"] = display_df["target_error_rate"].apply(lambda x: f"{x:.1f}%")

            st.dataframe(
                display_df[["customer_name", "date_added", "target_error_rate"]],
                use_container_width=True,
                hide_index=True,
            )

            st.markdown("---")
            st.markdown("### Set Target Error Rate by Customer")

            if not customers_df.empty:
                cust_name_for_target = st.selectbox("Select Customer", customers_df["customer_name"].tolist())
                target_choice = st.selectbox("Target Error Rate", ["3.0%", "2.0%", "1.0%"])
                target_value = float(target_choice.replace("%", ""))

                if st.button("💾 Update Target Error Rate", type="primary"):
                    cust_id = customers_df[customers_df["customer_name"] == cust_name_for_target]["id"].values[0]
                    update_customer_target(cust_id, target_value)
                    st.success(f"✅ Updated target error rate for {cust_name_for_target} to {target_value:.1f}%")
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
            lambda row: f"{row['customer_name']} - {row['job_number']} - {pd.to_datetime(row['production_date']).strftime('%Y-%m-%d')} (ID: {row['id']})",
            axis=1,
        ).tolist()

        selected_job = st.selectbox("Select Job to Delete", ["-- Select --"] + job_options)

        if selected_job != "-- Select --":
            job_id = int(selected_job.split("ID: ")[1].rstrip(")"))
            job_details = df[df["id"] == job_id].iloc[0]

            c1, c2 = st.columns(2)
            with c1:
                st.write(f"**Customer:** {job_details['customer_name']}")
                st.write(f"**Job Number:** {job_details['job_number']}")
                st.write(f"**Date:** {pd.to_datetime(job_details['production_date']).strftime('%Y-%m-%d')}")
            with c2:
                st.write(f"**Pieces:** {int(job_details['total_pieces']):,}")
                st.write(f"**Damages:** {int(job_details['total_damages']):,}")
                st.write(f"**Error Rate:** {float(job_details['error_rate']):.2f}%")

            if st.button("🗑️ Delete This Job", type="primary"):
                delete_job(job_id)
                st.success("✅ Job deleted successfully!")
                st.rerun()


if __name__ == "__main__":
    main()
