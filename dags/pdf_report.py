from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2
from fpdf import FPDF

# PostgreSQL connection details
DB_HOST = "host.docker.internal"  # Access local system PostgreSQL from Docker
DB_PORT = 5432  # Default PostgreSQL port
DB_NAME = "demo_report"
DB_USER = "postgres"
DB_PASSWORD = "postgres"

# Directory to save PDF
# PDF_SAVE_PATH = "/mnt/d/reports"  # Mounted D drive directory
PDF_SAVE_PATH = "/mnt/d"  # Mounted D drive directory

def fetch_data_from_postgres():
    """Fetch data from PostgreSQL database."""
    try:
        connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        )
        cursor = connection.cursor()
        query = "SELECT * FROM employees;"  # Replace with your table/query
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        connection.close()
        return rows
    except Exception as e:
        raise RuntimeError(f"Error fetching data: {str(e)}")

def create_pdf_report(**context):
    """Create a PDF report from fetched data."""
    rows = context['ti'].xcom_pull(task_ids='fetch_data')
    if not rows:
        raise RuntimeError("No data found to generate the report")

    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", size=12)

    pdf.cell(200, 10, txt="Database Report", ln=True, align='C')

    # Adding rows to PDF
    for row in rows:
        pdf.cell(200, 10, txt=str(row), ln=True, align='L')

    # Save PDF
    pdf_file_path = f"{PDF_SAVE_PATH}/report_{datetime.now().strftime('%Y%m%d%H%M%S')}.pdf"
    pdf.output(pdf_file_path)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    "postgres_to_pdf_report",
    default_args=default_args,
    description="Fetch data from PostgreSQL and create a PDF report",
    schedule_interval="@daily",
    start_date=datetime(2024, 12, 1),
    catchup=False,
) as dag:
    fetch_data = PythonOperator(
        task_id="fetch_data",
        python_callable=fetch_data_from_postgres,
    )

    generate_pdf = PythonOperator(
        task_id="generate_pdf",
        python_callable=create_pdf_report,
        provide_context=True,
    )

    fetch_data >> generate_pdf
