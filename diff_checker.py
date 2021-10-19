import csvdiff
import pprint

patch = csvdiff.diff_files('diff_files/Moxi-Company-2018-11-26-v1.csv', 'diff_files/Moxi-Company-2018-11-26-v2.csv', ['uuid'], '|')
update_query = []
for key, data in enumerate(patch['changed']):
    columns = ''
    total_count = len(data['fields'])
    for inner_key,inner_data in enumerate(data['fields']):
        columns += inner_data+'='+'"'+data['fields'][inner_data]['to']+'"'
        if(inner_key < total_count-1):
            columns +=', '

    update_query.append('update company set {} where uuid="{}";'.format(columns, data['key'][0]))

print(update_query)

# pp = pprint.PrettyPrinter(indent=4)
# pp.pprint(patch)
# just show the changed rows
#print(patch)