import os
import smtplib
from email.message import EmailMessage


def send_email(
    subject,
    body,
    user=os.getenv("EMAIL_USERNAME"),
    pwd=os.getenv("EMAIL_PASSWORD"),
    recipients=[os.getenv("DEFAULT_RECIPIENT")],
):
    msg = EmailMessage()

    msg["Subject"] = subject
    msg["From"] = user
    msg["To"] = ", ".join(recipients)
    msg.set_content(body)

    with smtplib.SMTP_SSL(host="smtp.gmail.com", port=465) as server:
        server.login(user, pwd)
        server.sendmail(from_addr=user, to_addrs=recipients, msg=msg.as_string())
