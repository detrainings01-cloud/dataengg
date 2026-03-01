import argparse 

parser = argparse.ArgumentParser(description="Example script for argparse")
parser.add_argument('--input_file_name', type=str, required=True, help='Input file path')
parser.add_argument('--output_file_name', type=str, required=True, help='Output file path')

args = parser.parse_args()

print("*****************")
print(args.input_file_name)
print(args.output_file_name)