import boto3

# Parameters for instance creation
REGION_NAME = 'us-west-1'

# US WEST 1
if REGION_NAME == 'us-west-1':
    AMI_ID = "ami-0da424eb883458071"
    INSTANCE_TYPE = "t2.micro"
    SUBNET_ID = "subnet-0ec58214554aae2a0"
    KEY_NAME = "my_aws_key_us-west-1"
    SECURITY_GROUP_ID = "sg-022fc9d4e10ee30da"

# EU WEST 1
if REGION_NAME == 'eu-west-1':
    AMI_ID = "ami-0d64bb532e0502c46"
    INSTANCE_TYPE = "t2.micro"
    SUBNET_ID = "subnet-0dfabf5b6c47eb240"
    KEY_NAME = "my_aws_key_eu-west-1"
    SECURITY_GROUP_ID = "sg-0837568cd61c41bbf"

# EU WEST 2
elif REGION_NAME == 'eu-west-2':
    AMI_ID = "ami-0e8d228ad90af673b"
    INSTANCE_TYPE = "t2.micro"
    SUBNET_ID = "subnet-0e14a14e8d2948184"
    KEY_NAME = "my_aws_key_eu-west-2"
    SECURITY_GROUP_ID = "sg-04638beba3be30caf"

# Initialize a session using your credentials
session = boto3.Session(profile_name='default', region_name=REGION_NAME)
ec2 = session.resource('ec2')

# Create the EC2 instance
instance = ec2.create_instances(
    ImageId=AMI_ID,
    InstanceType=INSTANCE_TYPE,
    MaxCount=1,
    MinCount=1,
    KeyName=KEY_NAME,
    SubnetId=SUBNET_ID,
    SecurityGroupIds=[SECURITY_GROUP_ID],
    TagSpecifications=[
        {
            'ResourceType': 'instance',
            'Tags': [{'Key': 'Name', 'Value': 'MyFirstPublicInstance_'+REGION_NAME}],
        }
    ],
)

print(f"Instance {instance[0].id} is launching...")
print("Done")
