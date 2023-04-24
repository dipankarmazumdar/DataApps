import streamlit as st
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import GreaterThan
import plotly.express as px
import os

os.environ['AWS_DEFAULT_REGION'] = os.getenv('AWS_DEFAULT_REGION')
os.environ['AWS_ACCESS_KEY_ID'] = os.getenv('AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv('AWS_SECRET_ACCESS_KEY')


# Set up Streamlit app
st.set_page_config(page_title="PyIceberg Streamlit Example", layout="wide")

# Load db & tables from Iceberg catalog 
st.title(':blue[Apache Iceberg]  and :orange[DuckDB] :duck:')
st.image('st_news.png', width=1500)
st.markdown("<h3></h3><h3></h3>", unsafe_allow_html=True)
st.markdown("""
This Streamlit app is designed to play around with the Apache Iceberg Table format. You can
further run your own SQL queries & analyze data using DuckDB
""")
st.markdown("<h3></h3>", unsafe_allow_html=True)
catalog = load_catalog("glue", **{"type": "glue"})
st.subheader('Enter your :blue[Apache Iceberg] Table Name')
db = st.text_input('', 'test' + '.' + 'churn')
ice_table = catalog.load_table(db)
table_iden = ice_table.identifier
table_name = table_iden[1]



@st.cache_data
def iceberg_func(db):
    scan = ice_table.scan()
    scan = scan.to_pandas()
    return scan

if table_name=='churn':
    scan_new = iceberg_func(db)
    st.write(scan_new)
    st.write(f"Number of rows: {scan_new.shape[0]}")
    st.markdown("<h3></h3>", unsafe_allow_html=True)


    churn_count = scan_new['Churn'].value_counts().reset_index()
    churn_count.columns = ['Churn', 'Count']
    col1, col2 = st.columns(2)
    with col1:
        state_churn_count = scan_new.groupby(['State', 'Churn']).size().reset_index()
        state_churn_count.columns = ['State', 'Churn', 'Count']
        fig1 = px.bar(state_churn_count, x='State', y='Count', color='Churn')
        st.plotly_chart(fig1, use_container_width=True)

    with col2:
        fig2 = px.scatter(scan_new, x='Total_day_charge', y='Total_night_charge', color='Churn')
        st.plotly_chart(fig2)

    col3, col4 = st.columns(2)
    with col3:
        fig3 = px.bar(churn_count, x='Churn', y='Count', color='Churn')
        st.plotly_chart(fig3, use_container_width=True)

    with col4:
        fig4 = px.box(scan_new, x='Churn', y='Account_length')
        st.plotly_chart(fig4, use_container_width=True)

else:
    scan_new = iceberg_func(db)
    st.write(scan_new)
    st.write(f"Number of rows: {scan_new.shape[0]}")
    st.markdown("<h3></h3>", unsafe_allow_html=True)

    product_count = scan_new['product'].value_counts().reset_index()
    product_count.columns = ['product', 'Count']
    fig5 = px.bar(product_count, x='product', y='Count')
    st.plotly_chart(fig5)


# Expression operator:
# filter = st.text_input('Account Length greater than? ', '')
# sc_new = ice_table.scan(row_filter=GreaterThan("Account_length", filter)).to_pandas()

# Use the table scan to convert it into an in-memory DuckDB table
con = ice_table.scan().to_duckdb(table_name=table_name)

# Running a query using DuckDB with aggregations
st.subheader('Enter your :orange[Duck DB] Query')
query = st.text_input('', '')
if query=='':
    print("no value entered")
else:
    duck_val = con.execute(
        query
    ).df()
    st.write(duck_val)

# "SELECT Churn , AVG(CAST(Customer_service_calls AS INT)) AS avg_calls FROM churn GROUP BY Churn"