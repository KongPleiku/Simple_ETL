import boto3
import gradio as gr
import os
import tempfile
from io import BytesIO
import io
import pyperclip  

aws_access_key = "AKIAYLZZKK37VVCURPCU" 
aws_region = "ap-southeast-1"

s3 = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name=aws_region,
)

def list_buckets():
    """Lists available S3 buckets."""
    try:
        response = s3.list_buckets()
        buckets = [bucket['Name'] for bucket in response['Buckets']]
        return buckets
    except Exception as e:
        return [f"Error: {e}"]

def list_s3_objects(bucket_name, prefix=""):
    """Lists objects in an S3 bucket with a given prefix, simulating a file system."""
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter="/")
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

def navigate_s3(bucket_name, path, selected_item, object_list):
    """Navigates the S3 bucket based on user selection."""
    if selected_item:
        if selected_item.startswith("[folder]"):
            folder_name = selected_item.replace("[folder] ", "")
            new_path = os.path.join(path, folder_name) + "/"
            return new_path, gr.update(choices=list_s3_objects(bucket_name, new_path), value=None)
        elif selected_item.startswith("[file]"):
            file_name = selected_item.replace("[file] ", "")
            full_path = os.path.join(path, file_name)
            return full_path, gr.update(choices=list_s3_objects(bucket_name, path), value=None) #Refresh the list
    return path, gr.update(choices=list_s3_objects(bucket_name, path), value=None)

def go_back(bucket_name, path, object_list):
    """Goes back to the parent folder."""
    if path:
        if path.endswith('/'):
            new_path = os.path.dirname(os.path.dirname(path)) + "/" # go 2 levels up, and add '/'
            if new_path == "//":
                new_path = "" # reset to main folder if path is root after going back.
        else:
            new_path = os.path.dirname(path)
            if not new_path:
                new_path = ""
            else:
                new_path = new_path + "/"

        return new_path, gr.update(choices=list_s3_objects(bucket_name, new_path), value=None)
    return "", gr.update(choices=list_s3_objects(bucket_name, ""), value=None) # go to root if path is empty.

def change_bucket(bucket_name, path, object_list):
    """Changes the selected bucket."""
    return "", gr.update(choices=list_s3_objects(bucket_name, ""), value=None)

def copy_path(bucket_name, path):
    """Copies the current path to the clipboard."""
    full_path = f"s3://{bucket_name}/{path}" if path else f"s3://{bucket_name}/"
    pyperclip.copy(full_path)
    return f"Copied to clipboard: {full_path}"

def create_interface():
    """Creates the Gradio interface."""
    with gr.Blocks() as demo:
        bucket_name = gr.State("")
        path = gr.State("")
        with gr.Row():
            bucket_dropdown = gr.Dropdown(choices=list_buckets(), label="Select Bucket", value=None)
        with gr.Row():
            path_display = gr.Textbox(label="Current Path", value="", interactive=False)
            copy_button = gr.Button("Copy Path")
        with gr.Row():
            object_list = gr.Dropdown(choices=[], label="Objects", value=None, interactive=True) 
        with gr.Row():
            back_button = gr.Button("Back")
        with gr.Row():
            copy_output = gr.Textbox(label="Copy Output", interactive=False)

        bucket_dropdown.change(
            fn=change_bucket,
            inputs=[bucket_dropdown, path, object_list],
            outputs=[path, object_list],
        )

        object_list.select(
            fn=navigate_s3,
            inputs=[bucket_dropdown, path, object_list, object_list],
            outputs=[path, object_list],
        )

        back_button.click(
            fn=go_back,
            inputs=[bucket_dropdown, path, object_list],
            outputs=[path, object_list]
        )

        copy_button.click(
            fn=copy_path,
            inputs=[bucket_name, path],
            outputs=[copy_output]
        )

        path.change(fn=lambda x:x, inputs=path, outputs=path_display) 
        bucket_dropdown.change(fn=lambda x:x, inputs=bucket_dropdown, outputs=bucket_name) 

    return demo

if __name__ == "__main__":
    demo = create_interface()
    demo.launch()
