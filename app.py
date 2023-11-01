#Sabian-Dev
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError
from pymongo import MongoClient
import smtplib
from email.mime.text import MIMEText

# Kafka configuration
bootstrap_servers = 'localhost:9092'
topic = 'email_registration_topic'
otp_topic = 'otp_validation_topic'

# MongoDB configuration
mongo_host = 'localhost'
mongo_port = 27017
mongo_db = 'my_database'
mongo_collection = 'email_collection'

# Email configuration
smtp_server = 'smtp_server'
smtp_port = 587
smtp_username = 'smtp_username'
smtp_password = 'smtp_password'
sender_email = 'sender@x.com'

#Kafka topics if they don't exist
admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
topic_list = [NewTopic(name=topic, num_partitions=1, replication_factor=1),
              NewTopic(name=otp_topic, num_partitions=1, replication_factor=1)]
admin_client.create_topics(new_topics=topic_list, validate_only=False)

# Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# Kafka consumer
consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers)

# MongoDB client
mongo_client = MongoClient(mongo_host, mongo_port)
mongo_db = mongo_client[mongo_db]
mongo_collection = mongo_db[mongo_collection]

# Register email and send to Kafka
def register_email(email):
    try:
        producer.send(topic, value=email.encode('utf-8'))
        print(f"Email ID registration request sent: {email}")
    except KafkaError as e:
        print(f"Error sending registration request: {e}")

# Save email to Mongo
def save_email_to_mongodb(email):
    email_data = {
        'email': email,
        'otp': None
    }
    mongo_collection.insert_one(email_data)
    print(f"Email saved to MongoDB: {email}")

# Generate OTP and update Mongo
def generate_otp(email):
    otp = "123456"  
    mongo_collection.update_one({'email': email}, {'$set': {'otp': otp}})
    print(f"OTP generated and saved to MongoDB for email: {email}")
    send_otp_email(email, otp)

#Send email 
def send_otp_email(email, otp):
    message = MIMEText(f"Your OTP is: {otp}")
    message['Subject'] = 'OTP Verification'
    message['From'] = sender_email
    message['To'] = email

    try:
        smtp_obj = smtplib.SMTP(smtp_server, smtp_port)
        smtp_obj.starttls()
        smtp_obj.login(smtp_username, smtp_password)
        smtp_obj.sendmail(sender_email, [email], message.as_string())
        smtp_obj.quit()
        print("OTP sent successfully!")
    except smtplib.SMTPException as e:
        print(f"Error sending OTP email: {e}")

# Validate OTP using Mongo
def validate_otp(email, otp):
    result = mongo_collection.find_one({'email': email, 'otp': otp})
    if result:
        print(f"OTP validation result for email {email}: Valid")
    else:
        print(f"OTP validation result for email {email}: Invalid")

# email registration
email = input("Enter email address: ")
register_email(email)

# Consume messages from Kafka server
for message in consumer:
    email = message.value.decode('utf-8')
    save_email_to_mongodb(email)  
    generate_otp(email)  
    break 

# Validate OTP 
otp = input("Enter OTP: ")
validate_otp(email, otp)