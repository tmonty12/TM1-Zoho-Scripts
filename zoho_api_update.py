"""
Usage:
    zoho_api_update <cube_name> <context_dimension> <fields_dimension> <element_dimension> <base_context> <update_context> <fields> <api_fields> <server_name> <err_file_path> [<zoho_client_id>] [<zoho_client_secret>] [<zoho_refresh_token>] [<server_address>] [<server_port>] [<server_user>] [<server_password>]
    zoho_api_update -h
    zoho_api_update --version

Options:
    -h          Show this screen
    --version   Show version
"""

from TM1py import TM1Service, Subset, NativeView, AnonymousSubset
import json
from docopt import docopt
from Zoho import Zoho
from settings import CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN, get_server_credentials, write_err_msg


def delete_existing_view(tm1, cube_name, view_name):
    view_exists_tuple = tm1.views.exists(cube_name=cube_name, view_name=view_name)
    if view_exists_tuple[0] or view_exists_tuple[1]:
        tm1.views.delete(cube_name, view_name)


def create_view(tm1, cube_name, view_name, context_dimension_name, fields_dimension_name, element_dimension_name, fields_subset, context):
        view = NativeView(cube_name=cube_name, view_name=view_name)

        element_dimension_subset = create_subset_all(dimension_name=element_dimension_name)
        view.add_row(dimension_name=element_dimension_name, subset=element_dimension_subset)
        
        context_subset = Subset(subset_name='z{}_TM1py'.format(context), dimension_name=context_dimension_name, elements=[context])
        if tm1.subsets.exists(subset_name='z{}_TM1py'.format(context), dimension_name=context_dimension_name):
            tm1.subsets.delete(subset_name='z{}_TM1py'.format(context), dimension_name=context_dimension_name)
        tm1.subsets.create(subset=context_subset)

        view.add_column(dimension_name=context_dimension_name, subset=context_subset)
        view.add_row(dimension_name=fields_dimension_name, subset=fields_subset)

        tm1.views.create(view=view)


def create_subset_all(dimension_name):
    expression = '{TM1FilterByLevel({ TM1SubsetAll([%s]) }, 0)}'%(dimension_name)
    return AnonymousSubset(dimension_name=dimension_name, expression=expression)


def create_update_json_obj(update_values_df, fields_dimension_name, element_dimension_name, fields, api_fields):
    if update_values_df.size == 0:
        return

    obj = { 'data': [] }
    for idx, values in update_values_df.iterrows():
        if element_idx(obj, idx) == -1:
            id = values[element_dimension_name][1:]
            obj['data'].append({'id' : id })
        field = api_fields[fields.index(values[fields_dimension_name])]
        obj['data'][element_idx(obj, idx)][field] = values['Data']
        

    return json.dumps(obj)


def element_idx(obj, id):
    for i, element in enumerate(obj['data']):
        if element['id'] == id:
            return i
    return -1
    

def update_base_context(tm1, cube_name, update_values, context_dimension_name, base_context):
    update_values.insert(1, context_dimension_name, base_context)

    tm1.cells.write_dataframe(cube_name=cube_name, data=update_values)


def cleanup(tm1, cube_name, base_context, update_context, context_dimension_name, fields_dimension_name):
    # Delete temp views
    tm1.views.delete(cube_name=cube_name, view_name='z{}_TM1py'.format(base_context))
    tm1.views.delete(cube_name=cube_name, view_name='z{}_TM1py'.format(update_context))

    # Delete temp subsets
    tm1.subsets.delete(subset_name='z{}_TM1py'.format(base_context), dimension_name=context_dimension_name)
    tm1.subsets.delete(subset_name='z{}_TM1py'.format(update_context), dimension_name=context_dimension_name)
    tm1.subsets.delete(subset_name='zFields_TM1py', dimension_name=fields_dimension_name)


def main(args):
    """
        This program takes the project dimension and its measure dimension with a third dimension for context - either the Current in Zoho
        or the Updates to be sent to Zoho, find the differences between the Update and Current, send these changes to
        the Zoho API and then update the Current context to match the Update context.

        Parameters
        ----------

        Notes
        -----
        Assumes that the dimension order is: element_dimension, context_dimension, fields_dimension
    """
    fields = args['<fields>'].split(',')
    api_fields = args['<api_fields>'].split(',')

    SERVER_ADDRESS, SERVER_PORT, SERVER_USER, SERVER_PASSWORD = get_server_credentials(args['<server_name>'], args['<server_address>'], args['<server_port>'], args['<server_user>'], args['<server_password>'])

    with TM1Service(address=SERVER_ADDRESS, port=SERVER_PORT, user=SERVER_USER, password=SERVER_PASSWORD, ssl=True) as tm1:
        base_view_name = 'z{}_TM1py'.format(args['<base_context>'])
        update_view_name = 'z{}_TM1py'.format(args['<update_context>'])

        delete_existing_view(tm1, args['<cube_name>'], base_view_name)
        delete_existing_view(tm1, args['<cube_name>'], update_view_name)

        # Create a subset of fields to compare
        if tm1.subsets.exists(subset_name='zFields_TM1py', dimension_name=args['<fields_dimension>']):
            tm1.subsets.delete(subset_name='zFields_TM1py', dimension_name=args['<fields_dimension>'])
        fields_subset = Subset(subset_name='zFields_TM1py', dimension_name=args['<fields_dimension>'], elements=fields)
        tm1.subsets.create(subset=fields_subset)

        # Create the base and update views to compare
        create_view(tm1, args['<cube_name>'], base_view_name, args['<context_dimension>'], \
                        args['<fields_dimension>'], args['<element_dimension>'], fields_subset, args['<base_context>'])
        create_view(tm1, args['<cube_name>'], update_view_name, args['<context_dimension>'], \
                        args['<fields_dimension>'], args['<element_dimension>'], fields_subset, args['<update_context>'])
        
        # Create dfs for base values and update values and make sure column labels match
        base_values = tm1.cells.execute_view_dataframe_shaped(cube_name=args['<cube_name>'], view_name=base_view_name).rename(columns={ args['<base_context>']: 'Data' })
        update_values = tm1.cells.execute_view_dataframe_shaped(cube_name=args['<cube_name>'], view_name=update_view_name).rename(columns={ args['<update_context>']: 'Data' })
        base_values = base_values.fillna(0)
        update_values = update_values.fillna(0)

        # Grab rows that don't match between dfs - these are the ones we want to update
        update_values = update_values.loc[(base_values != update_values).any(axis=1)]
        json_data = create_update_json_obj(update_values, args['<fields_dimension>'], args['<element_dimension>'], fields, api_fields)

        if json_data:
            zoho = Zoho(CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN)
            zoho.update_potentials(json_data)
            
            # After updating zoho, update the base context
            update_base_context(tm1, args['<cube_name>'], update_values, args['<context_dimension>'], args['<base_context>'])
        
        cleanup(tm1, args['<cube_name>'], args['<base_context>'], args['<update_context>'], args['<context_dimension>'], args['<fields_dimension>'])

if __name__ == "__main__":
    args = docopt(__doc__, version='1.0.0')
    try:
        main(args)
    except KeyError as e:
        err_msg = 'KeyError: The column key {} you provided does not exist in Pipeline file'.format(e)
        write_err_msg(args, err_msg)
    except Exception as e:
        err_msg = '{}: {}'.format(type(e).__name__, e)
        write_err_msg(args, err_msg)