FROM python:3.12-slim

WORKDIR /app

# Copy just the requirements file to leverage Docker cache
COPY ./requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

EXPOSE 8080

# Streamlit runs on port 8080 and disables protections for Cloud Run
ENV STREAMLIT_SERVER_PORT=8080 \
    STREAMLIT_SERVER_ENABLECORS=false \
    STREAMLIT_SERVER_ENABLEWEB_SOCKET_COMPRESSION=false \
    STREAMLIT_SERVER_ENABLE_XSRF_PROTECTION=false \
    STREAMLIT_LOGGER_LEVEL=debug

CMD ["streamlit", "run", "streamlit_app.py", "--server.port=8000", "--server.address=0.0.0.0"]
