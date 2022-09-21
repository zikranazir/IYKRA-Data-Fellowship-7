FROM python:3.10.4 as compiler
ENV PYTHONUNBUFFERED 1

WORKDIR /app

RUN python3 -m venv /opt/venv
# Enable venv
ENV PATH="/opt/venv/bin:$PATH"

# Install dependencies:
COPY requirements.txt .
RUN . /opt/venv/bin/activate 
RUN pip install -r requirements.txt

FROM python:3.10.4 as runner
WORKDIR /app
COPY --from=compiler /opt/venv /opt/venv

# Run the application:
ENV PATH="/opt/venv/bin:$PATH"
COPY uploader.py .
CMD . /opt/venv/bin/activate && exec python uploader.py
