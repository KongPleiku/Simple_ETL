import boto3
import gradio as gr


S3Region = ["us-east-1", "us-east-2", "us-west-1", "us-west-2", "ap-south-1", "ap-northeast-2", "ap-southeast-1", "ap-southeast-2", "ap-northeast-1", "ca-central-1", "eu-central-1", "eu-west-1", "eu-west-2", "sa-east-1"]
class S3Connection:
    def __init__(self,access_key: str, secret_key: str, region: str):
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        self.connected = False
    
    def connect(self):
        try:
            self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region
            )
            
            self.connected = True
            return True
        except Exception as e:
            print(e)
            self.connected = False
            return False
    
    def list_Buckets(self):
        rp = self.s3_client.list_buckets()
        bucketsNames = [bucket['Name'] for bucket in rp['Buckets']]
        return bucketsNames
    
    def list_object(self, bucket_name: str, prefix=""):
        try:
            response = self.s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter="/")
            contents = response.get("Contents", [])
            common_prefixes = response.get("CommonPrefixes", [])

            items = []
            if common_prefixes:
                for p in common_prefixes:
                    folder_name = p["Prefix"].split("/")[-2] if p["Prefix"].endswith("/") else p["Prefix"].split("/")[-1]
                    items.append(f"[folder] {folder_name}")

            if contents:
                for c in contents:
                    key = c["Key"]
                    file_name = key.split("/")[-1]
                    if file_name:
                        items.append(f"[file] {file_name}")

            return items
        except Exception as e:
            return [f"Error: {e}"]
        
        
    def __str__(self):
        return "S3 Connection"

class S3Manager:
    def __init__(self):
        self.connection_Dict = {}
    
    def create_Storage_Connection(self, ConnectionName: str, AccessKey: str, SecretKey: str, Region: str):
        temp = S3Connection(AccessKey, SecretKey, Region)
        if temp.connect():
            self.connection_Dict[ConnectionName] = temp
            gr.Info("Created " + ConnectionName + " Connection and Successfull Connected")
            return True
        else:
            gr.Info("Failed to Create" + ConnectionName + " Connection")
            return False
    
    def get_Storage_Connection(self, ConnectionName: str) -> S3Connection:
        return self.connection_Dict[ConnectionName]
    
    def show_Storage_Connections(self):
        connection_list = []
        for name, connection in self.connection_Dict.items():
            status = "Connected" if connection.connected else "Disconnected"
            connection_list.append([name, connection.region, status])
        return connection_list
    
    def list_Storage_Connections(self):
        return list(self.connection_Dict.keys())
    
    def load_Storage_Connection(self):
        with open('D:\VS_prj\SparkETLRepo\Logic_Loom\API\dev\storage.txt', 'r') as file:
            for line in file:
                line = line.strip()
                if line:
                    name, access_key, secret_key, region = line.split(',', 3)
                    self.create_Storage_Connection(name.strip(), access_key.strip(), secret_key.strip(), region.strip())