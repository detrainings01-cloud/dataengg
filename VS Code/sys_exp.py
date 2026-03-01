import sys 
import os
print(sys.argv)


if len(sys.argv) != 3:
    sys.exit("This module is expecting 3 arguments")

input_file_name = sys.argv[1]
output_file_name = sys.argv[2]
print("*****************")
print(input_file_name)
print(output_file_name)

#  agr parse       #  No Validation 
#if not os.path.exists(output_file_name):
#    sys.exit("File not found")
    
print(sys.path)