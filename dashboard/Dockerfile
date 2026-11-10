FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PULSECART_HOME=/opt/pulsecart

WORKDIR /opt/pulsecart
COPY dashboard/requirements.txt /tmp/dashboard-requirements.txt
RUN pip install --no-cache-dir -r /tmp/dashboard-requirements.txt
COPY dashboard/app.py /opt/pulsecart/dashboard/app.py
COPY dashboard/sample_reports /opt/pulsecart/dashboard/sample_reports

EXPOSE 8501
CMD ["streamlit", "run", "/opt/pulsecart/dashboard/app.py", "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true"]
