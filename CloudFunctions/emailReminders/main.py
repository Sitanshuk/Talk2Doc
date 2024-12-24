import functions_framework
from google.cloud import pubsub_v1
from datetime import datetime, timedelta
import sendgrid



def callback(message):
    current_time = int(time.time())
    due_date = int(message.attributes.get('deadline', 0))
    
    if due_date <= current_time + 86400:  # Within next 24 hours
        print(f"Processing message: {message.data}")
        message.ack()
    else:
        # Nack the message, it will be redelivered later
        message.nack()

@functions_framework.http
def alerting(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """
    project_id = "midterm-440408"
    subscription_id = "alerting_system"

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    sendgrid_api = "YOUR_SENDGRID_API_KEY"
    sg = sendgrid.SendGridClient("YOUR_SENDGRID_API_KEY")
    message = sendgrid.Mail()

    try:
        # Pull messages
        response = subscriber.pull(
            request={"subscription": subscription_path}
        )
        for received_message in response.received_messages:
            message_data = received_message.message.data.decode("utf-8")
            message_data = json.loads(message_data)
            if type(message_data) == list:
                if (message_data.get("status") in {"OA", "Interview", "Offer"}) and (message_data.get("deadline") != ""):
                    deadline_str = message.attributes.get('deadline', '')
                    try:
                        deadline_date = datetime.strptime(deadline_str, '%Y-%m-%d').date()
                        if deadline_date <= current_date + timedelta(days=2):
                            to_email = message_data.get('email', None)
                            position = message_data.get('position', None)
                            status = message_data.get('status', None)
                            deadline = message_data.get('deadline', None)
                            email_message = f"""
                                Your {status} is approaching deadline on {deadline}
                                for the following role
                            """

                            #send email here
                            message.add_to(to_email)
                            message.set_from("noreply@talk2doc.com")
                            message.set_subject("Action Required: Deadline Approaching")
                            message.set_html(email_message)
                            sg.send(message)

                            subscriber.acknowledge(
                                request={
                                    "subscription": subscription_path,
                                    "ack_ids": [received_message.ack_id],
                                }
                             )
                    except Exception as e:
                        print(e)

    except Exception as e:
        print(e)
    
    return 'Hello {}!'.format(name)
