import base64
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from quickstart import get_service
from googleapiclient.errors import HttpError


def send_email(subject: str, message: str):
    """
    Send email to yourself
    :param subject:
    :param message:
    :return:
    """
    try:
        service = get_service()
        new_message = MIMEMultipart()
        new_message['To'] = 'DubnovMenaiot@gmail.com'
        new_message['From'] = 'DubnovMenaiot@gmail.com'
        new_message['Subject'] = subject
        msg = MIMEText(message)
        new_message.attach(msg)

        raw = base64.urlsafe_b64encode(new_message.as_bytes())
        raw = raw.decode()
        body = {'raw': raw}
        sent_message = service.users().messages().send(userId="me", body=body).execute()
        print(F'Message Id: {sent_message["id"]}')

    except HttpError as error:
        print(F'An error occurred: {error}')


if __name__ == '__main__':
    for i in range(10):
        send_email('Test subject', 'Test message')
