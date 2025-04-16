-- STEP 1: Membuat Notification Integration untuk Mengirim Email
CREATE OR REPLACE NOTIFICATION INTEGRATION EMAIL_NOTIFICATION
TYPE = EMAIL
ENABLED = TRUE
ALLOWED_RECIPIENTS = ('dimas.nuriza@berca.co.id');

-- STEP 2: Membuat Stage untuk Menyimpan File CSV
CREATE OR REPLACE STAGE my_stage;

-- Berikan izin akses ke Stage
GRANT READ, WRITE ON STAGE my_stage TO ROLE ACCOUNTADMIN;

-- STEP 3: Menyimpan Data ke Stage dalam Format CSV
COPY INTO @my_stage/data.csv
FROM (SELECT * FROM DATA_PENJUALAN.DATA_DUMMY_PENJUALAN)
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"')
SINGLE = TRUE;

-- STEP 4: Membuat Stored Procedure untuk Mengirim Email Tanpa Lampiran
CREATE OR REPLACE PROCEDURE SEND_CSV_EMAIL_NOTIFICATION()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
    // Subjek & isi email
    var subject = "CSV Data from Snowflake";
    var to_address = "dimas.nuriza@berca.co.id";  // Ganti dengan email tujuan
    var body = `
        <html>
        <head></head>
        <body>
        <h2>CSV Data from Snowflake</h2>
        <p>Silakan akses Snowflake Stage untuk mengunduh file CSV.</p>
        <p>Path file: <b>@my_stage/data.csv</b></p>
        </body>
        </html>
    `;

    // Query untuk mengirim email (Tanpa Lampiran)
    var send_email_query = `
        CALL SYSTEM$SEND_EMAIL(
            'EMAIL_NOTIFICATION',
            '${to_address}',
            '${subject}',
            '${body}',
            'text/html'
        );
    `;
    
    // Eksekusi query untuk mengirim email
    var stmt = snowflake.createStatement({ sqlText: send_email_query });
    stmt.execute();

    return "✅ Email berhasil dikirim (tanpa lampiran).";
} catch (err) {
    return "❌ Error: " + err.message;
}
$$;

-- STEP 5: Menjalankan Prosedur untuk Mengirim Email
CALL SEND_CSV_EMAIL_NOTIFICATION();