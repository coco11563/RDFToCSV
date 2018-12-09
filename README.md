# SeqN3CSVProcessor
RDF to csv format


using the rdf4j package to match the *.n3 file to Statement

after the statement seq generate , the program will handle and encapsulate the statement to Node

during the step listed before , the program will generate an forest

by using the forest, the csv string seq will be build



this program can totally handle the OBJECT part of rdf

but can't convert the OWL part or semantic part


any problems you found and any needs of friendly you seek, please feel free to contact me 

coco11563@yeah.net

usage

'''
if we need to process the n3 file 
the script to start spark process should be like
spark-submit ______sparkconfig______ /n3filepath /outputpath
the n3 file should be named like *.n3
the out put file will be csv format, and split by every 1G n3 file content
'''
