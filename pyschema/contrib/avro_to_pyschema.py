field_map = {
             'string': 'pyschema.Text',
             'float': 'pyschema.Float',
             'double': 'pyschema.Float',
             'int': 'pyschema.Integer',
             'boolean': 'pyschema.Boolean',
             'long': 'pyschema.Integer',
            }
extra_args_map = {
                  'float': ['size=4'],
                  'int': ['size=4'],
                  }
complex_field_map = {
             'array': 'pyschema.List',
             'map': 'pyschema.Map',
             'record': 'pyschema.SubRecord',
             }

def get_ununionized_field_type(field_type):
    if isinstance(field_type, list):
        if len(field_type) != 2 or field_type[0] != 'null':
            raise Exception("PySchema doesn't support such advanced union types yet: %r" % field_type)
        return field_type[1]
    return field_type

def get_name(field):
    if isinstance(field['type'], basestring):
        return field['name']
    if isinstance(field['type'], list):
        return field['type'][0]['name']
    if isinstance(field['type'], dict):
        return field['type']['name']

def is_nullable(field_type):
    if isinstance(field_type, list):
        return True
    return False

def get_field_type_name(field_type):
    field_type = get_ununionized_field_type(field_type)
    if isinstance(field_type, dict):
        field_type = field_type['type']
    return field_type

def get_sub_field_type(field):
    field_type = get_ununionized_field_type(field['type'])
    type_name = get_field_type_name(field['type'])
    if type_name == 'record':
        return field_type['fields']
    elif type_name == 'array':
        return field_type['items']
    elif type_name == 'map':
        return field_type['values']

def get_field_definition(field, sub_records):
    args = []
    if isinstance(field, basestring):
        field_type = field
        args.append('nullable=False')
    else:
        field_type = get_field_type_name(field['type'])
        if not is_nullable(field['type']):
            args.append('nullable=False')
    # simple types
    if field_type in field_map.keys():
        args.extend(extra_args_map.get(field_type, []))
        return "%s(%s)" % (field_map[field_type], ', '.join(args))

    # complex types
    elif field_type == 'record':
        args.insert(0, get_name(field))
        sub_rec = get_pyschema_record(field, sub_records)
        sub_records.append(sub_rec)
        return "%s(%s)" % (complex_field_map[field_type], ', '.join(args))
    elif field_type in complex_field_map.keys():
        sub_field = get_sub_field_type(field)
        args.insert(0, get_field_definition(sub_field, sub_records))
        return "%s(%s)" % (complex_field_map[field_type], ', '.join(args))

def get_pyschema_record(schema, sub_records):
    name = get_name(schema)

    record_def = "class %s(pyschema.Record):\n" % name
    if is_nullable(schema['type']):
        fields = schema['type'][0]['fields']
    else:
        fields = schema['fields']
    for field in fields:
        name = field['name']
        record_def += "    %s = %s\n" % (name, get_field_definition(field, sub_records))
    return record_def
