# PubSub

## Qos 

PubQos | SubQos | In Message | Out Message
-------|--------|------------|-------------
   0   |   0    |   -        | - 
   0   |   1    |   -        | - 
   0   |   2    |   -        | - 
   1   |   0    |   -        | - 
   1   |   1    |   -        | - 
   1   |   2    |   -        | - 
   2   |   0    |   -        | - 
   2   |   1    |   -        | - 
   2   |   2    |   -        | - 


## Publish


## Performance

Mac Air(11): 

Function     | Time(microseconds)
-------------|--------------------
match        | 6.25086
triples      | 13.86881
words        | 3.41177
binary:split | 3.03776

iMac:

Function     | Time(microseconds)
-------------|--------------------
match        | 3.2348
triples      | 6.93524
words        | 1.89616
binary:split | 1.65243

