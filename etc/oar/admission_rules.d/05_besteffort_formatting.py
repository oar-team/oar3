# Formatting besteffort cases
if ('besteffort' in types) and queue != 'besteffort':
    queue = 'besteffort'
    print('[ADMISSION RULE] Automatically redirect in the besteffort queue')

if queue == 'besteffort' and ('besteffort' not in types):
    types.append('besteffort')
    print('[ADMISSION RULE] Automatically add the besteffort type')
    
if 'besteffort' in types:
    if properties:
        properties = "({}) AND besteffort = 'YES'".format(properties)   
    else:
        properties = "besteffort = 'YES'"
    print('[ADMISSION RULE] Automatically add the besteffort constraint on the resources')
