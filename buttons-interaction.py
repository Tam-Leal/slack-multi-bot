from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import os
from slack_bolt import App
from dotenv import load_dotenv
from slack_bolt.adapter.socket_mode import SocketModeHandler
import openai
import time
import pandas as pd
import io
from redshift import main
from flask import Flask, jsonify

# Load environment variables
load_dotenv()

api_key = os.environ.get('OPENAI_API_KEY')
SLACK_BOT_TOKEN = os.environ.get('SLACK_BOT_TOKEN')
SLACK_APP_TOKEN = os.environ.get('SLACK_APP_TOKEN')

# Set up OpenAI and Slack app
openai.api_key = api_key
slack_app = App(token=SLACK_BOT_TOKEN)

app = Flask(__name__)


# Rota de saúde para o Render verificar se o serviço está ativo
@app.route("/health")
def health_check():
    return jsonify({"status": "ok"}), 200


# Rota raiz para evitar erros 404
@app.route("/")
def index():
    return "Hello, World!"


def send_buttons(channel_id):
    client = WebClient(token=os.environ["SLACK_BOT_TOKEN"])
    try:
        blocks = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "Choose an action:"
                }
            },
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "Search for missing images"
                        },
                        "action_id": "missing_images",
                        "value": "missing_images"
                    },
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "Incorrect sentences"
                        },
                        "action_id": "wrong_sentences",
                        "value": "wrong_sentences"
                    },
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "Misspelled words"
                        },
                        "action_id": "misspelled_words",
                        "value": "misspelled_words"
                    },
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "Generate CSV"
                        },
                        "action_id": "generate_csv",
                        "value": "generate_csv"
                    },
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "Generate Redshift CSV"
                        },
                        "action_id": "generate_redshift_csv",
                        "value": "generate_redshift_csv"
                    },
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "Count to Ten"
                        },
                        "action_id": "count_to_ten",
                        "value": "count_to_ten"
                    }
                ]
            }
        ]
        response = client.chat_postMessage(
            channel=channel_id,
            text="Please choose an action from the buttons below.",
            blocks=blocks
        )
    except SlackApiError as e:
        print(f"Error: {e}")


def count_to_ten(channel_id):
    client = WebClient(token=os.environ["SLACK_BOT_TOKEN"])
    response = client.chat_postMessage(channel=channel_id, text="Counting: 0")
    ts = response['ts']
    for i in range(1, 11):
        time.sleep(1)
        new_text = f"Counting: {i}"
        client.chat_update(channel=channel_id, ts=ts, text=new_text)


def generate_csv_and_upload(channel_id):
    data = {
        "Name": ["Alice", "Bob", "Charlie"],
        "Age": [25, 30, 35],
        "Job": ["Engineer", "Doctor", "Artist"]
    }
    df = pd.DataFrame(data)
    output = io.BytesIO()
    df.to_csv(output, index=False, encoding='utf-8')
    output.seek(0)
    client = WebClient(token=os.environ["SLACK_BOT_TOKEN"])
    try:
        client.files_upload_v2(
            channel=channel_id,
            file=output,
            filename="report.csv",
            title="Your CSV Data"
        )
    except SlackApiError as e:
        print(f"Error uploading file: {e}")
    finally:
        output.close()


@slack_app.action("generate_csv")
def handle_generate_csv(ack, body, say):
    ack()
    say(text="Processing your request, please wait... :hourglass_flowing_sand:")
    channel_id = body['channel']['id']
    generate_csv_and_upload(channel_id)


@slack_app.action("generate_redshift_csv")
def handle_generate_redshift_csv(ack, body, say):
    ack()
    channel_id = body['channel']['id']
    say(text="Processing your Redshift CSV request, please wait... :hourglass_flowing_sand:")
    try:
        df = main()
        if df is not None:
            output = io.BytesIO()
            df.to_csv(output, index=False, encoding='utf-8')
            output.seek(0)
            client = WebClient(token=os.environ["SLACK_BOT_TOKEN"])
            client.files_upload_v2(
                channel=channel_id,
                file=output,
                filename="redshift_output.csv",
                title="Your Redshift CSV Data"
            )
            output.close()
            say(text="CSV generated from Redshift and uploaded successfully.")
        else:
            say(text="Failed to generate the CSV from Redshift.")
    except Exception as e:
        say(f"An error occurred while generating or uploading the CSV: {e}")


@slack_app.event("message")
def handle_message_events(body, say):
    event = body['event']
    if 'subtype' in event or 'app_mention' in event.get('type'):
        return
    send_buttons(event['channel'])


@slack_app.action("missing_images")
def handle_missing_images(ack, body, say):
    ack()
    say(text="Running script to search for missing images...")


@slack_app.action("text_formatting")
def handle_text_formatting(ack, body, say):
    ack()
    say(text="Running script to correct text formatting...")


@slack_app.action("wrong_sentences")
def handle_wrong_sentences(ack, body, say):
    ack()
    say(text="Running script to correct incorrect sentences...")


@slack_app.action("misspelled_words")
def handle_misspelled_words(ack, body, say):
    ack()
    say(text="Running script to correct misspelled words...")


@slack_app.action("count_to_ten")
def handle_count_to_ten(ack, body, say):
    ack()
    say(text="We are running your request, please wait... :hourglass_flowing_sand:")
    channel_id = body['channel']['id']
    time.sleep(2)
    count_to_ten(channel_id)


if __name__ == "__main__":
    handler = SocketModeHandler(slack_app, SLACK_APP_TOKEN)
    handler.start()

# if __name__ == "__main__":
#     from threading import Thread
#
#
#     def run_flask():
#         port = int(os.environ.get("PORT", 3000))
#         app.run(host="0.0.0.0", port=port)
#
#
#     flask_thread = Thread(target=run_flask)
#     flask_thread.start()
#
#     handler = SocketModeHandler(slack_app, SLACK_APP_TOKEN)
#     handler.start()
