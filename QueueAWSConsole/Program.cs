using Amazon.SQS;
using Amazon.SQS.Model;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace QueueAWSConsole
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("SQS Demonstration");
            List<MessageModel> messages = CreateMessage();
            Console.WriteLine("Creating Queue");
            AmazonSQSClient sqsClient = new AmazonSQSClient(Amazon.RegionEndpoint.USEast1);
            string queueUrl = CreateQueue(sqsClient, "DemoQueue", "10");
            Console.WriteLine("Queue Created, Url: "+queueUrl);
            Console.WriteLine("Press enter to continue");
            Console.ReadLine();

            Console.WriteLine("Sending messages to SQS");
            foreach(MessageModel message in messages)
            {
                string msg = JsonConvert.SerializeObject(message);
                SendMessageResponse sendMessageResponse = SendMessage(msg, queueUrl, sqsClient);
                Console.WriteLine($"Message :{message.Id} Sent to queue. " +
                    $"HTTP response code {sendMessageResponse.HttpStatusCode.ToString()}");

            }
            Console.WriteLine("Finished sending messages to SQS. Press Enter to continue");
            Console.ReadLine();

            Console.WriteLine("Starting to read messages from SQS");

            ReceiveMesssage(queueUrl, sqsClient);

            Console.WriteLine("Finished reading messages from SQS");
        }
        /// <summary>
        /// Process Message in a queue
        /// </summary>
        /// <param name="queueUrl">SQS queue URL returned when a queue is created</param>
        /// <param name="sqsClient">AmazonSQSClient object with an AWS region specified</param>
        /// <exception cref="NotImplementedException"></exception>
        private static void ReceiveMesssage(string queueUrl, AmazonSQSClient sqsClient)
        {
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
            receiveMessageRequest.QueueUrl = queueUrl;

            int counter = 0;
            int length = NumberOfMessagesInQueue(queueUrl, sqsClient);//number of messages read in a queue defaults to 1 unless otherwise specified
            while(counter < length)
            {
                ReceiveMessageResponse receiveMessageResponse = Task.Run(async () => await sqsClient.ReceiveMessageAsync(receiveMessageRequest)).Result;
                if(receiveMessageResponse.HttpStatusCode == System.Net.HttpStatusCode.OK)
                {
                    Message message = receiveMessageResponse.Messages[0];

                    MessageModel msg = JsonConvert.DeserializeObject<MessageModel>(message.Body);

                    //Write object properties to the console
                    Console.WriteLine("****************************************");
                    Console.WriteLine($"SQS Message Id: {message.MessageId.ToString()}");
                    Console.WriteLine($"Message Id: {msg.Id}");
                    Console.WriteLine($"Message Description: {msg.Description}");
                    Console.WriteLine($"Message Created Date: {msg.CreatedOn.ToString()}");
                    Console.WriteLine($"****************************************");
                    Console.WriteLine();
                }
                counter++;
            }
        }
        /// <summary>
        /// Inspect a queue attributes and get an appropriate number of messages in the queue
        /// </summary>
        /// <param name="queueUrl">SQS queue URl returned when a queue is created</param>
        /// <param name="sqsClient">AmazonSQSClient object with an AWS region specified</param>
        /// <returns>An integer value of the approximate number of messages in a queue</returns>
        /// <exception cref="NotImplementedException"></exception>
        private static int NumberOfMessagesInQueue(string queueUrl, AmazonSQSClient sqsClient)
        {
            int retval = 0;

            GetQueueAttributesRequest getQueueAttributesRequest = new GetQueueAttributesRequest();
            getQueueAttributesRequest.QueueUrl = queueUrl;
            getQueueAttributesRequest.AttributeNames.Add("ApproximateNumberOfMessages");

            GetQueueAttributesResponse response = Task.Run(async () => await sqsClient.GetQueueAttributesAsync(getQueueAttributesRequest)).Result;
            retval = response.ApproximateNumberOfMessages;
            return retval;
        }

        /// <summary>
        /// Send a Message to a queue
        /// </summary>
        /// <param name="msg">Message Object Serialized to Json format. 256kb limit</param>
        /// <param name="queueUrl">SQS queue URL returned when a queue is created</param>
        /// <param name="sqsClient">Amazon SQS Client object with an AWS region specified</param>
        /// <returns>Send message response object containing meta data of message transmission</returns>
        /// <exception cref="NotImplementedException"></exception>
        private static SendMessageResponse SendMessage(string msg, string queueUrl, AmazonSQSClient sqsClient)
        {
            SendMessageRequest sendMessageRequest = new SendMessageRequest();
            sendMessageRequest.QueueUrl = queueUrl;
            sendMessageRequest.MessageBody = msg;

            SendMessageResponse sendMessageResponse = Task.Run(async () => await sqsClient.SendMessageAsync(sendMessageRequest)).Result;

            return sendMessageResponse;
        }

        private static List<MessageModel> CreateMessage()
        {
            List<MessageModel> retval = new List<MessageModel>();
            int counter = 0;
            int length = 100;

            while(counter < length)
            {
                MessageModel temp = new MessageModel(counter, "I am message #:" + counter.ToString());
                retval.Add(temp);

                counter ++;
            }
            return retval;
            
        }
        /// <summary>
        /// Create a queue for processing data
        /// </summary>
        /// <param name="sqsClient">AmazonSQSClient object with an AWS region specified</param>
        /// <param name="queueName">Name of the queue we wish to create</param>
        /// <param name="visibilityTimeout">value of a message visibility timeout in seconds</param>
        /// <returns>URL of an Amazon SQS queue created</returns>
        /// <exception cref="NotImplementedException"></exception>
        private static string CreateQueue(AmazonSQSClient sqsClient, string queueName, string visibilityTimeout)
        {
            CreateQueueRequest createQueueRequest = new CreateQueueRequest();
            createQueueRequest.QueueName = queueName;

            Dictionary<string, string> attrs = new Dictionary<string, string>();
            attrs.Add(QueueAttributeName.VisibilityTimeout, visibilityTimeout);
            createQueueRequest.Attributes = attrs;

            CreateQueueResponse createQueueResponse = Task.Run(async () => await sqsClient.CreateQueueAsync(createQueueRequest)).Result;
            return createQueueResponse.QueueUrl;
        }
    }

}
