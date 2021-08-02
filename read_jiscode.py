import json
import re

jiscodes = {}
with open('jiscode.rb', 'r', encoding='Shift_JIS') as f:
    pattern = re.compile(r'jcodes\[\'(\d+)\'\] = \'(.*)\'')
    for line in f:
        m = pattern.search(line)
        if m:
            jiscodes[m.group(1)] = m.group(2).split(',')

with open('jiscode.json', 'w', encoding='utf-8') as f:
    json.dump(jiscodes, f, ensure_ascii=False)




