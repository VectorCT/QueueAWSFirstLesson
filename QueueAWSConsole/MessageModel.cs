namespace QueueAWSConsole
{
    public class MessageModel
    {
        public int Id { get; set; }
        public string Description { get; set; }
        public DateTime CreatedOn { get; set; }

        public MessageModel(int Id, string Description)
        {
            this.Id = Id;
            this.Description = Description;
            this.CreatedOn = DateTime.Now; 
        }
    }
}