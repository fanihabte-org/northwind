FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir fastapi==0.115.* "uvicorn[standard]==0.32.*" "pandas>=2.2" pyarrow duckdb
# Copy the package recursively: Bulk API modules live in a subpackage and
# must be present when the image is run without the development bind mount.
COPY . /app/fakeforce/
EXPOSE 8080
CMD ["uvicorn", "fakeforce.app:app", "--host", "0.0.0.0", "--port", "8080"]
