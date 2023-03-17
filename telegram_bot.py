import socket
import errno
import telebot
import logging
import threading
from config import DEBUG_TELEGRAM_API_KEY, clients

TELEGRAM_INDENT = "   "

logger = logging.getLogger()
telegram_bot = telebot.TeleBot(DEBUG_TELEGRAM_API_KEY, parse_mode='HTML')
connections = {}

# Create server
server_socket = socket.socket()
server_socket.bind(('localhost', 5000))
server_socket.listen(3)


def listen_client(conn, client):
    order = False
    while True:
        try:
            # receive data stream. it won't accept data packet greater than 1024 bytes
            message = str(conn.recv(1024).decode())

            if not message:
                # if data is not received break
                break

            print(f"Message from {client} : {message}")
            message = message.split('|')

            if len(message) == 2:
                order = message[0] == 'True'
                message = message[1]
            else:
                message = message[0]

            send_telegram_message(client=client, message=message, order=order)

        except socket.error as error:
            if error.errno == errno.WSAECONNRESET:
                print(f'Disconnected from {client}')
                connections.pop(client, None)
                return
            else:
                raise Exception(error)

        except Exception:
            logger.exception(f'Exception listening to {client} client')

    conn.close()


def send_telegram_message(client: str, message: str, order: bool):
    chat_id = clients[client]['OrderChatId'] if order else clients[client]['MainChatId']
    message = message.replace('\t', TELEGRAM_INDENT)

    if message == 'send_performance_graph':
        telegram_bot.send_photo(chat_id, open(f"{client}_performance_graph.png", 'rb'))
    else:
        i = 2
        while len(message) > 4096:
            telegram_bot.send_message(chat_id=chat_id, text=message[:4096])
            message = message[4096:]
            message = f'(Part {i}) ' + message
            i += 1

        telegram_bot.send_message(chat_id=chat_id, text=message)


@telegram_bot.message_handler(commands=['hello_world', 'get_errors', 'error_acknowledged', 'get_cutoffs',
                                        'get_updates', 'get_budget', 'get_balance', 'get_orders', 'get_positions',
                                        'get_momentum', 'get_performance_graph'])
def handle_telegram_request(message):
    try:
        client = [key for key, details in clients.items() if details['MainChatId'] == message.chat.id][0]
        if client not in connections:
            send_telegram_message(client=client, message='Bot is not alive', order=False)
        else:
            print(f'Forwarding message to {client}: {message.text}')
            connections[client].sendall(message.text.encode())

    except Exception:
        logger.exception(f"Exception in handle_telegram_request")


def accept_thread():
    while True:
        c, adr = server_socket.accept()  # accept new connection
        cl = [key for key in clients if clients[key]['Port'] == adr[1]][0]
        print(f"Connection from {cl}")
        connections[cl] = c
        threading.Thread(target=listen_client, args=(c, cl)).start()


threading.Thread(target=accept_thread).start()
telegram_bot.infinity_polling()
