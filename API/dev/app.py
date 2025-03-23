import gradio as gr
import webbrowser
import threading
import os
import pandas as pd
from pymongo import MongoClient
import time 

import MongoDBPack
import StoragePack

connectionManager = MongoDBPack.MongodbManager()
storageManager = StoragePack.S3Manager()

connectionManager.loadConnection()
storageManager.load_Storage_Connection()

def launch_browser():
    import time
    time.sleep(1)
    webbrowser.open("http://127.0.0.1:7860")

def stop_app():
    demo.close()
    print("App stopped.")

def update_connections_info():
    return connectionManager.showConnections()

def fetch_data_with_progress(connection_name, database_name, collection_name, progress=gr.Progress()):
    """Fetches data from a collection with a progress bar."""
    connection = connectionManager.getConnection(connection_name)
    total_documents = connection.count_documents(database_name, collection_name)
    
    if total_documents == 0:
        return []
    
    batch_size = 100
    fetched_data = connection.fetch_data_preview(database_name, collection_name, batch_size)
    return fetched_data

with gr.Blocks() as demo:
    gr.Markdown("# Easy ETL and File Pusher")
    with gr.Tab("Storage Connection"):
        with gr.Accordion("Connection Manager"):
            with gr.Row(variant= "compact"):
                with gr.Column():
                    connection_name = gr.Text(label="Connection Name")
                    connection_Access_key = gr.Text(label="Connection Access Key")
                    connection_Secret_key = gr.Text(label="Connection Secret Key")
                    connection_Region = gr.Dropdown(choices=StoragePack.S3Region, label="Connection Region", interactive=True)
                    
                    create_btn = gr.Button("Create")
                    create_btn.click(fn= storageManager.create_Storage_Connection,
                                     inputs=[connection_name, connection_Access_key, connection_Secret_key, connection_Region],
                                     outputs=None)
                    
                with gr.Column():
                    connections_info = gr.Dataframe(
                        headers=["Name", "Region", "Status"],
                        interactive=False
                        )
                    update_button = gr.Button("Update Connections Info")
                    update_button.click(fn=storageManager.show_Storage_Connections, outputs=connections_info)
    
    with gr.Tab("Connection Manager"):
        with gr.Accordion("Data Source Connection"):
            gr.Markdown("## Create Connection")
            with gr.Row(variant= "compact"):
                with gr.Column():
                    connection_name = gr.Text(label="Connection Name")
                    connection_uri = gr.Text(label="Connection URI")
                    create_btn = gr.Button("Create")
                    create_result = gr.Textbox(label="Creation Result")
                    create_btn.click(
                        fn=connectionManager.createConnection,
                        inputs=[connection_name, connection_uri],
                        outputs=create_result
                    )

                with gr.Column():
                    connections_info = gr.Dataframe(
                        headers=["Name", "URI", "Status"],
                        interactive=False
                    )
                    update_button = gr.Button("Update Connections Info")
                    update_button.click(fn=update_connections_info, outputs=connections_info)
            
            gr.Markdown("## Data Explore")
            with gr.Row():
                def update_connections():
                        return gr.update(choices=connectionManager.listConnections()) 
                    
                def update_databases(connection_name):
                        connection = connectionManager.getConnection(connection_name)
                        databases = connection.ListDataBases()
                        return gr.update(choices=databases)
                    
                def update_collections(connectionName, databaseName):
                        connection = connectionManager.getConnection(connectionName)
                        collections = connection.ListCollections(databaseName)
                        return gr.update(choices=collections)
                    
                connection_dropdown = gr.Dropdown(choices=[], label="Select Connection", interactive=True, scale=1)
                            
                db_dropdown = gr.Dropdown(choices=[], label="Select Database", interactive=True, scale=1)
                connection_dropdown.change(update_databases, inputs=[connection_dropdown], outputs=[db_dropdown])
                            
                col_dropdown = gr.Dropdown(choices=[], label="Select Collection", interactive=True, scale=1)
                db_dropdown.change(update_collections, inputs=[connection_dropdown, db_dropdown], outputs=[col_dropdown])
                            
                reset_btn = gr.Button("🔄", size=0)
                reset_btn.click(update_connections, outputs=connection_dropdown)
            
            fetch_btn = gr.Button("Preview Table")
            with gr.Accordion("DataTable"):
                data_table = gr.Dataframe(label="Collection Data", wrap=True,interactive=False)
                        
            fetch_btn.click(fn=fetch_data_with_progress, inputs=[connection_dropdown, db_dropdown, col_dropdown], outputs=data_table)
        
        with gr.Accordion("Data Storage Connection"):
            gr.Markdown("## Create Connection")
            with gr.Row(variant= "compact"):
                with gr.Column():
                    connection_name = gr.Text(label="Connection Name")
                    connection_Access_key = gr.Text(label="Connection Access Key")
                    connection_Secret_key = gr.Text(label="Connection Secret Key")
                    connection_Region = gr.Dropdown(choices=StoragePack.S3Region, label="Connection Region", interactive=True)
                    
                    create_btn = gr.Button("Create")
                    create_btn.click(fn= storageManager.create_Storage_Connection,
                                     inputs=[connection_name, connection_Access_key, connection_Secret_key, connection_Region],
                                     outputs=None)
                    
                with gr.Column():
                    connections_info = gr.Dataframe(
                        headers=["Name", "Region", "Status"],
                        interactive=False
                        )
                    update_button = gr.Button("Update Connections Info")
                    update_button.click(fn=storageManager.show_Storage_Connections, outputs=connections_info)
            
            gr.Markdown("File Explorer")
            with gr.Column():
                bucket_name = gr.State("")
                path = gr.State("")
                
            
    with gr.Tab("Custom ETL Function"):
        pass
    with gr.Tab("Push File"):
        pass

    with gr.Accordion("See Details"):
        gr.Markdown("lorem ipsum")

    exit_button = gr.Button("Exit App", variant="stop")  # Red stop button
    exit_button.click(stop_app)

threading.Thread(target=launch_browser, daemon=True).start()
demo.launch()
