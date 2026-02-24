FROM python:3.11

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["functions-framework", "--target=converter_xlsx_para_csv"]
