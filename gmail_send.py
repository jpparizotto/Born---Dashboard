# gmail_send.py
# Envia e-mail pela sua conta Gmail com anexo (usando Gmail API)

import os
import base64
from email.message import EmailMessage

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.auth.transport.requests import Request


SCOPES = ["https://www.googleapis.com/auth/gmail.send"]


def _get_gmail_service():
    creds = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)

    # Se o token tiver expirado ou não existir, renova
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # Primeiro login: precisa do credentials.json baixado do Google Cloud
            from google_auth_oauthlib.flow import InstalledAppFlow

            flow = InstalledAppFlow.from_client_secrets_file(
                "credentials.json", SCOPES
            )
            creds = flow.run_local_server(port=0)

        with open("token.json", "w") as token:
            token.write(creds.to_json())

    service = build("gmail", "v1", credentials=creds)
    return service


def send_email_with_attachment(
    to_emails: list[str],
    subject: str,
    body_text: str,
    filename: str,
    file_bytes: bytes,
    from_email: str | None = None,
):
    service = _get_gmail_service()

    msg = EmailMessage()
    msg["To"] = ", ".join(to_emails)
    msg["Subject"] = subject
    if from_email:
        msg["From"] = from_email

    msg.set_content(body_text)

    # Anexo
    maintype = "application"
    subtype = "vnd.openxmlformats-officedocument.spreadsheetml.sheet"

    msg.add_attachment(
        file_bytes,
        maintype=maintype,
        subtype=subtype,
        filename=filename,
    )

    encoded_message = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    send_body = {"raw": encoded_message}

    sent = service.users().messages().send(userId="me", body=send_body).execute()
    return sent.get("id")
