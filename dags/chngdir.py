import os

def dir_chng(path: str = "/root/airflow/jobs"):

    
    if not os.path.exists(path):
        os.makedirs(path) # Create directory if it doesn't exist
    os.chdir(path)
    
    # Verify the change (optional)
    print(f"Current working directory: {os.getcwd()}")