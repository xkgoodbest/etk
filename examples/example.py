# This is how we intend to use it 
import sys
import os
sys.path.insert(1, os.path.join(sys.path[0], '..'))
import time
import etk

tk = etk.init()

start_time = time.time()

# Load all the dictionaries here
tk.load_dictionaries()

end_time = time.time()

print "Time taken to load all the tries: {0}".format(end_time - start_time)

print "\nCity Dictionary Extractor"
print tk.extract_using_dictionary(['portland'], name='cities')

print "\nHair Color Dictionary Extractor"
print tk.extract_using_dictionary(['brunette'], name='haircolor')

print "\nEthicities Dictionary Extractor"
print tk.extract_using_dictionary(['caucasian'], name='ethnicities')

print "\nEye Color Dictionary Extractor"
print tk.extract_using_dictionary(['brown'], name='eyecolor')

print "\nNames Dictionary Extractor"
print tk.extract_using_dictionary(['june'], name='names')

# API methods to be decided