import os 
import sys 
import argparse
import logging
from datetime import datetime

logging.basicConfig(filename='sys_util.log', filemode='a', 
                    level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def system_info():
    """Print and log system information."""
    logging.info("Fetching system information")
    print("Python Version:", sys.version)
    print("Platform:", sys.platform)
    print("Current Working Directory:", os.getcwd())
    logging.info(f"Python Version: {sys.version}")
    logging.info(f"Platform: {sys.platform}")
    logging.info(f"Current Working Directory: {os.getcwd()}")

def create_directory(dir_name):
    """Create a directory if it doesn't exist."""
    try:
        os.makedirs(dir_name, exist_ok=True)
        logging.info(f"Directory '{dir_name}' created or already exists.")
    except Exception as e:
        logging.error(f"Error creating directory '{dir_name}': {e}")
        sys.exit(f"Error creating directory '{dir_name}': {e}")

def delete_directory(dir_name):
    """Delete a directory if it exists."""
    try:
        if os.path.exists(dir_name):
            os.rmdir(dir_name)
            logging.info(f"Directory '{dir_name}' deleted.")
        else:
            logging.warning(f"Directory '{dir_name}' does not exist.")
    except Exception as e:
        logging.error(f"Error deleting directory '{dir_name}': {e}")
        sys.exit(f"Error deleting directory '{dir_name}': {e}")        

def create_file(file_name, content=""):
    """Create a file with specified content."""
    try:
        with open(file_name, 'w') as f:
            f.write(content)
        logging.info(f"File '{file_name}' created with content.")
    except Exception as e:
        logging.error(f"Error creating file '{file_name}': {e}")
        sys.exit(f"Error creating file '{file_name}': {e}")

def read_file(file_name):
    """Read and return the content of a file."""
    try:
        with open(file_name, 'r') as f:
            content = f.read()
        logging.info(f"Content of '{file_name}': {content}")
    except Exception as e:
        logging.error(f"Error reading file '{file_name}': {e}")
        sys.exit(f"Error reading file '{file_name}': {e}")

def append_file(file_name, content):
    """Append content to a file."""
    try:
        with open(file_name, 'a') as f:
            f.write(content)
        logging.info(f"Appended content to '{file_name}'.")
    except Exception as e:
        logging.error(f"Error appending to file '{file_name}': {e}")
        sys.exit(f"Error appending to file '{file_name}': {e}")

def delete_file(file_name):
    """Delete a file if it exists."""
    try:
        if os.path.exists(file_name):
            os.remove(file_name)
            logging.info(f"File '{file_name}' deleted.")
        else:
            logging.warning(f"File '{file_name}' does not exist.")
    except Exception as e:
        logging.error(f"Error deleting file '{file_name}': {e}")
        sys.exit(f"Error deleting file '{file_name}': {e}")

def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="System Utility Tool")
    parser.add_argument('--action', type=str, required=True, 
                        choices=['create_dir', 'delete_dir', 'create_file', 'read_file', 'append_file', 'delete_file', 'sys_info'],
                        help='Action to perform')
    parser.add_argument('--name', type=str, help='Name of the file or directory')
    parser.add_argument('--content', type=str, help='Content to write or append to a file')
    
    return parser.parse_args()

if __name__ == "__main__": 
    args = parse_arguments()
    
    if args.action == 'create_dir' and args.name:
        create_directory(args.name)
    elif args.action == 'delete_dir' and args.name:
        delete_directory(args.name)
    elif args.action == 'create_file' and args.name:
        create_file(args.name, args.content or "")
    elif args.action == 'read_file' and args.name:
        read_file(args.name)
    elif args.action == 'append_file' and args.name and args.content:
        append_file(args.name, args.content)
    elif args.action == 'delete_file' and args.name:
        delete_file(args.name)
    elif args.action == 'sys_info':
        system_info()
    else:
        logging.error("Invalid arguments provided.")
        sys.exit("Invalid arguments provided. Please check the usage.")