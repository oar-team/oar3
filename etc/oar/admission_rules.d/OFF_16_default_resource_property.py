# If resource types are not specified, then we force them to default
flag_property = False
for mold in resource_request:
    for resource_desc_lst in mold[0]:
        property_ = resource_desc_lst['property']
        if not re.match('.*type[\\s]=.*', property_):
            flag_property = True
            if not property_:
                resource_desc_lst['property'] = "type='default'"
            else:
                resource_desc_lst['property'] = "({}) AND type='default'"\
                                                                .format(property_)
                                                                
if flag_property:
    print('[ADMISSION RULE] Modify resource description with type constraints')
