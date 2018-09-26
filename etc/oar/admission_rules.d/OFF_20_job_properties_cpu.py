if 'sandy' in types:
    if properties:
        properties = "({}) AND cputype = 'sandybridge'".format(properties)
    else:
        properties = "cputype = 'sandybridge'"
else:
    if properties:
        properties = "({}) AND cputype = 'westmere'".format(properties)
    else:
        properties = "cputype = 'westmere'"
