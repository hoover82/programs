
# python C:\dan\learn\python\dictionaries.py


def reverse_dict ( dict ):
    ret = {}
    for i,key  in enumerate ( dict) :
        ret [dict[key]] = key	
        print ( i,key,dict[key])
    return dict
	
def duplicate_keys ( dict):
    x=1
	
DISTRIBUTORS ={
1: 'Williams'
 , 2: 'Camfour'
 ,24: 'HIcks, Inc.'
 ,4: 'Bangers'
, 24: 'Duplicate'
 }

print (len(DISTRIBUTORS))
print (DISTRIBUTORS)

new_dict = reverse_dict ( DISTRIBUTORS) 
print ( new_dict )
