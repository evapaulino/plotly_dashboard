

"""
BesserEsser Plotly Dashboard.
Description and explanations for the code will be placed her

- callbacks must be on this module
- selectors must not have a None default value
- for debugging mode change the value of DEBUG in the constants.py
- app.run(debug=DEBUG)  must be commented out when deploying on a server
"""


from dash import Dash, html, Input, Output, State, callback, ctx, no_update
from dash.exceptions import PreventUpdate
#import dash_bootstrap_components as dbc

from units import css, header, unit_0, unit_1, unit_2, unit_3, unit_4, unit_5, unit_6, unit_7, unit_8, footer
from data_processing import check_account, get_dataframes
from data_filtering import subset_data_by_dates, subset_data_by_selector_values
from table_toolkit import read_json, to_json, to_list_of_dicts
from table_toolkit import make_statistics_table, make_diary_table, prettify_diary_table, make_probably_bad_foods_table
from developer_toolkit import get_callback_args, get_default_values, make_handy_namespace
from computations import get_dates_range
from plotting_toolkit import (make_figure_3, make_figure_4, make_figure_5, make_figure_6, 
                                make_figure_7, make_figure_test)
from plotting_toolkit import make_figure

from constants import DEBUG, ERR_PREFIX, ACCOUNT, A, B, C, D, E  # values of selectors for reference


# Instantiate an application object
#app = Dash(__name__, external_stylesheets=[dbc.themes.SLATE])  # if dash_bootstrap_components are used
app = Dash(__name__)

# Define the layout
app.layout = html.Div([header, 
                       unit_0, 
                       unit_1, 
                       unit_2,  # basic statistics table
                       unit_3,
                       unit_4, 
                       unit_5, # units can be arranged into bootstrap columns
                       unit_6, # or commented out if not needed
                       unit_7,
                       unit_8,
                       footer], 
                       style=css)



##### CALLBACK FUNCTIONS #####

# UNIT 0: the "Konto Suchen" section
@callback(  #this is a plotly-dash decorator
    Output(component_id='unit_0_message_1', component_property='children'), # not found message
    Output(component_id='unit_0_message_2', component_property='children'),
    Output(component_id='unit_1_selector_1', component_property='min_date_allowed'),       # date picker
    Output(component_id='unit_1_selector_1', component_property='max_date_allowed'),       # date picker
    Output(component_id='unit_1_selector_1', component_property='initial_visible_month'),  # date picker
    Output(component_id='unit_8_table_1', component_property='data'),
    Output(component_id='store_1', component_property='data'),
    Output(component_id='store_2', component_property='data'),
    Output(component_id='store_3', component_property='data'),
    Input(component_id='unit_0_button', component_property='n_clicks'),
    State(component_id='unit_0_inputbox', component_property='value'),
    prevent_initial_call=True)
def update_unit_0(n_clicks, value):
    """
    TODO: docs

    Searches for the passed in account (value)
    Returns messages (not found / no data / yes data)
    Saves the data in the "store" dash-components (basically in the user's browser session)

    n_clicks: int (just a placeholder, i.e. will not be used in this function at all)
    value: None or int
    """
    # the ´value´ is the value of the dcc.Input-box i.e. the account number
    if not value:
        raise PreventUpdate  # plotly-dash thing
    
    # Check the account
    res = check_account(account_id=value)

    if res is ValueError:  # will not be raised - just to tell that the input is bad - to avoid SQl injection
        unit_0_message_1 = "Ungültige Eingabe"
        return (unit_0_message_1, *([no_update]*8))  # count the Output objects
    elif res is None:
        unit_0_message_1 = f"{ACCOUNT} {value} nicht gefunden"
        return (unit_0_message_1, *([no_update]*8))  # count the Output objects
    elif res is False:
        unit_0_message_1 = f"Keine Informationen für {ACCOUNT} {value} vorhanden"
        return (unit_0_message_1, *([no_update]*8))  # count the Output objects
    else:
        unit_0_message_1 = ""
        unit_0_message_2 = f"Informationen über {ACCOUNT} {value}"  # h-tag header


    # Get the data from the data base
    df_eating, df_symptoms, *_ = get_dataframes(account_id=value)  # returns two df's
    
    # Make the diary table 
    df_diary = make_diary_table(df_eating, df_symptoms)

    # Get min max dates to prettify the date picker
    min_date, max_date = get_dates_range(df_eating, df_symptoms)

    # Get the data for the "Probably bad foods table"
    data_probably_bad_foods = to_list_of_dicts(make_probably_bad_foods_table(df_eating))

    # Store the data (as str in json format) in the user's browser session
    json_eating = to_json(df_eating)  # str in json format
    json_symptomreport = to_json(df_symptoms)  # str in json format
    json_diary = to_json(df_diary)

    # Update the corresponding dash components with these values:
    # (must correspond to the `Output` arguments in the decorator above)
    return (unit_0_message_1,         #Output(component_id='unit_0_message_1', component_property='children')
            unit_0_message_2,         #Output(component_id='unit_0_message_2', component_property='children')
            min_date,                 #Output(component_id='unit_1_selector_1', component_property='min_date_allowed')
            max_date,                 #Output(component_id='unit_1_selector_1', component_property='max_date_allowed')
            max_date,                 #Output(component_id='unit_1_selector_1', component_property='initial_visible_month')
            data_probably_bad_foods,  #Output(component_id='unit_8_table_1', component_property='data')
            json_eating,              #Output(component_id='store_1', component_property='data')
            json_symptomreport,       #Output(component_id='store_2', component_property='data')
            json_diary)               #Output(component_id='store_3', component_property='data')





# UNIT 1: Zeitraum wählen
@callback(get_callback_args(unit_1, parent=None))
def update_unit_1(*components):
    """
    TODO: docs
    The 2 selectors (date-range-picker and dropdown) are updated
    either by the trigger on data saving (into users browser session)
    or by the user manually
    """
    # which unit is this callback function for?
    UNIT = unit_1

    # initially data = None (before anything is stored into the user's browser session)
    if None in components: 
        raise PreventUpdate

    # convert to namespace obejct for handy indexing, attr get/set, mutability and iterability
    components = make_handy_namespace(components)  # for mutability, attr-access etc, iterability...
 
    # Read data fom json stored in user's browser session
    df_eating = read_json(components.json_eating)
    df_symptoms = read_json(components.json_symptomreport)


    ##### Updating the selectors (date range picker and dropdown) #####
    # these values will be necessary for that:
    
    # default values for the selectors dynamically (to reset the selector on data reload)
    default_values = get_default_values(UNIT)

    # the number of default_values == the number of (updatable) selector in this unit
    i = len(components) - len(default_values)  # will be used as index later

    # min and max dates of the user's history for the date picker selector
    min_date, max_date = get_dates_range(df_eating, df_symptoms)

    ### If the function call is triggered by data being saved in the dcc.Store ###
    # i.e. the user clicked the "Suchen" button and data was saved by `update_unit_0` unction
    if ctx.triggered_id == 'store_1':
        components[i:] = default_values
        # update the date range picker to the min/max of the df at hand
        components.start_date, components.end_date  = min_date, max_date
    
    ### If the function call is triggered by the user manually changing the selectors' values ###
    # 'unit_1_selector_1' is the date picker
    if ctx.triggered_id == 'unit_1_selector_1':  # the date picker range is the trigger
        components.timespan_dropdown = default_values[2]   # reset the dropdown to its default
    # 'unit_1_selector_2' is the dropdown menu
    elif ctx.triggered_id == 'unit_1_selector_2':  # the drop down menu for time span
        dropdown_value = components.timespan_dropdown
        if dropdown_value == A:  # the date picker range values - the first/default option
            return (*components[i:], no_update, no_update)
        elif dropdown_value == B: # all history
            components.start_date, components.end_date = min_date, max_date
        elif dropdown_value == C:  # last 7*4*3 days of available data
            components.start_date, components.end_date = get_dates_range(max_date, 7*4*3)
        elif dropdown_value == D:  # last 7*4 days of available data
            components.start_date, components.end_date = get_dates_range(max_date, 7*4)
        elif dropdown_value == E:  # last 7 days of available data
            components.start_date, components.end_date = get_dates_range(max_date, 7)
        elif True:
            "extend the logic here, in case the dropdown menu is extended with new options"
    elif ctx.triggered_id == 'unit_1_selector_X':   # for example a new check-box on unit_1
        "add functionality for unit_1_selector_X"
        "you will find what you need" in components

    #here? TODO
    # make and save statistics (df_eating_subset_by_dates, df_symptoms_subset_by_dates)

    # update the unit
    return components[i:]  # the last i elements
 



# UNIT 2: UNIT 2: Statistics / Usage overview
@callback(get_callback_args(unit_2, parent=unit_1))
def update_unit_2(*components):
    """
    output: 1 element (unit_2_table_1.data)
    """
    # which unit is this callback function for?
    UNIT = unit_2

    # initially data = None (before anything is stored into the user's browser session)
    if None in components: 
        raise PreventUpdate

    # for mutability, attr-access etc, iterability
    components = make_handy_namespace(components)
 
    # Get the default values for the selectors (this code block is not needed - just for consistency)
    default_values = get_default_values(UNIT)
    i = len(components) - len(default_values)
    
    # Reset the selectors  (this code block is not needed - just for consistency)
    if ctx.triggered_id == 'store_1':
        components[i:] = default_values


    # Read data fom json stored in user's browser session
    df_eating = read_json(components.json_eating)
    df_symptoms = read_json(components.json_symptomreport)

    # Subset the data by dates
    df_eating_subset_dates = subset_data_by_dates(df_eating,
                                                start_date=components.start_date,
                                                end_date=components.end_date)
    df_symptoms_subset_dates = subset_data_by_dates(df_symptoms,
                                                         start_date=components.start_date,
                                                         end_date=components.end_date)

    # Make the statistics table
    df_statistics = make_statistics_table(df_eating_subset_dates, df_symptoms_subset_dates)
    data_statistics_table = to_list_of_dicts(df_statistics)

    # update
    return (*components[i:],        # []  but is there for consistency
            data_statistics_table)
    




# UNIT3: Diary (graph + table)
@callback(get_callback_args(unit_3, parent=unit_1))
def update_unit_3(*components):
    """
    TODO docs
    """

    # Which unit is this callback function for?
    unit = unit_3

    # initially data = None (before anything is stored into the user's browser session)
    if None in components: 
        raise PreventUpdate  # not actually raise but cought by plotly-dash

    # for mutability, attr get/set etc
    components = make_handy_namespace(components)  # for mutability, attr-access etc, iterability...
 
    # Read data fom json stored in user's browser session
    df_eating = read_json(components.json_eating)
    df_symptoms = read_json(components.json_symptomreport)
    df_diary = read_json(components.json_diary)

    # Get the default values for the selectors
    default_values = get_default_values(unit)   # len(default_values) == 0
    i = len(components) - len(default_values)   # just for consistency

    # This section would deal with updateing the selectors / setting their default values
    # but on this unit there are no selectors

    # Filter the data in the df's
    df_eating_subset_by_dates = subset_data_by_dates(df_eating, 
                                                     start_date=components.start_date,
                                                     end_date=components.end_date)
    df_symptoms_subset_by_dates = subset_data_by_dates(df_symptoms, 
                                                            start_date=components.start_date,
                                                            end_date=components.end_date)
    # make the plot
    fig = make_figure(unit, 
                      df_eating_subset_by_dates, df_symptoms_subset_by_dates,
                      debugging_info=components)
    #Diary table
    df_diary_subset_by_dates = subset_data_by_dates(df_diary, 
                                                    start_date=components.start_date,
                                                    end_date=components.end_date)
    df_diary_subset_by_dates = prettify_diary_table(df_diary_subset_by_dates)  # must be done exactly here
    
    # Convert to list of dicts to be passed into a plotly-dahs DataTable
    data_diary = to_list_of_dicts(df_diary_subset_by_dates)

    # Data for the plotly-dash DataTable (in debug mode)
    data_debugging_table = to_list_of_dicts(df_eating_subset_by_dates)

    # update the unit
    return (*components[i:],     # [] but is there for consistency
            fig,
            data_diary,          # list of dicts for the diary table
            data_debugging_table)



# UNIT 4: Welche Lebensmittel sind am meisten konsumiert
@callback(get_callback_args(unit_4, parent=unit_1))
def update_unit_4(*components):
    """
    Use this code block as a template.

    Updates a figure (or multiple figures) in a single "unit" and
    resets the selectors belonging to this unit on data relaod.
    This function is decorated with dash's ‘callback‘ function.

    The code block for this function (including the decorator) can be used as a template
    to define further callback functions (after you have created a new dash "unit").
    The dash unit must be a dash.dcc.Div object, whose children are dash core components as well as
    dash.html elements.

    For this to work:
     - pass your ‘unit‘ into the  ‘get_callback_args‘ function (see above)
     - assign your new unit to the UNIT constant variable below (UNIT = unit_n)
    (The ‘get_callback_args‘ function returns the arguments necessray for the decorator)

    Args:
        data: jsonified pandas.DataFrame as str
        component_values: values passed in from the selectors
    
    Returns:
        component_values defined for Output, as well as plotly figures
    """

    # Which unit is this callback function for?
    unit = unit_4

    # initially data = None (before anything is stored into the user's browser session)
    if None in components: 
        raise PreventUpdate

    # for mutability, attr-access etc, iterability
    components = make_handy_namespace(components)
 
    # Read data fom json stored in user's browser session
    df_eating = read_json(components.json_eating)

    # Get the default values for the selectors
    default_values = get_default_values(unit)
    i = len(components) - len(default_values)  # will be used as index later
    
    # Reset the selectors if ‘store‘ is the trigger
    if ctx.triggered_id == 'store_1':
        components[i:] = default_values

    # Subset the data by dates
    df_subset_dates = subset_data_by_dates(df_eating,
                                           start_date=components.start_date,
                                           end_date=components.end_date)

    # Subset by the two selectors on this unit
    df_subset_dates_and_selectors = subset_data_by_selector_values(df_subset_dates,
                                                                   meals_selector=components.selector1,
                                                                   symptom_selector=components.selector2)
    # Make the plot
    fig = make_figure(unit, df_subset_dates_and_selectors,
                            color=components.selector2,      # all days/days with symptom...
                            debugging_info=components)       # used in DEBUG mode

    # Data for the plotly-dash DataTable (in debug mode)
    data_debugging_table = to_list_of_dicts(df_subset_dates_and_selectors)

    # update
    return (*components[i:], 
            fig, 
            data_debugging_table)



# UNIT 5: Welche Lebensmittel wurden unmittelbar vor der Symptomentstehung gegessen
@callback(get_callback_args(unit_5, parent=unit_1))
def update_unit_5(*components):
    """
    You can use this as a template fro a new unit
    """

    # Which unit is this callback function for?
    unit = unit_5

    # initially data = None (before anything is stored into the user's browser session)
    if None in components: 
        raise PreventUpdate

    # for mutability, attr-access etc, iterability
    components = make_handy_namespace(components)
 
    # Read data fom json stored in user's browser session
    df_eating = read_json(components.json_eating)

    # Get the default values for the selectors
    default_values = get_default_values(unit)
    i = len(components) - len(default_values)  # will be used as index later
    
    # Reset the selectors if ‘store‘ is the trigger
    if ctx.triggered_id == 'store_1':
        components[i:] = default_values

    # Subset the data by dates
    df_subset_dates = subset_data_by_dates(df_eating,
                                           start_date=components.start_date,
                                           end_date=components.end_date)

    # Subset by the two selectors on this unit
    df_subset_dates_and_selectors = subset_data_by_selector_values(df_subset_dates,
                                                                   meals_selector=components.selector1,
                                                                   severity_selector=components.selector2)
    # Make the plot
    fig = make_figure(unit, df_subset_dates_and_selectors,
                            color='red',                     # all red
                            debugging_info=components)       # used in DEBUG mode

    # Data for the plotly-dash DataTable (in debug mode)
    data_debugging_table = to_list_of_dicts(df_subset_dates_and_selectors)

    # update the Graph
    return (*components[i:], 
            fig, 
            data_debugging_table)



# UNIT 6: Wie sieht ein typisches Frühstück, Mittagessen oder Abendessen aus
@callback(get_callback_args(unit_6, parent=unit_1))
def update_unit_6(*components):
    """
    TODO: docs
    """

    # which unit is this callback function for?
    unit = unit_6

    # initially data = None (before anything is stored into the user's browser session)
    if None in components: 
        raise PreventUpdate

    # for mutability, attr-access etc, iterability
    components = make_handy_namespace(components)
 
    # Read data fom json stored in user's browser session
    df_eating = read_json(components.json_eating)

    # Get the default values for the selectors
    default_values = get_default_values(unit)
    i = len(components) - len(default_values)  # will be used as index later
    
    # Reset the selectors if ‘store‘ is the trigger
    if ctx.triggered_id == 'store_1':
        components[i:] = default_values

    # Subset the data by dates
    df_subset_dates = subset_data_by_dates(df_eating,
                                           start_date=components.start_date,
                                           end_date=components.end_date)
    # Subset by the two selectors on this unit
    df_subset_dates_and_selectors = subset_data_by_selector_values(df_subset_dates,
                                                                   meals_selector=components.selector1,
                                                                   symptom_selector=components.selector2)
    # Make the plot
    fig = make_figure(unit, df_subset_dates_and_selectors,
                            color=components.selector2,
                            debugging_info=components)

    # Data for the plotly-dash DataTable (in debug mode)
    data_debugging_table = to_list_of_dicts(df_subset_dates_and_selectors)

    # update the Graph
    return (*components[i:], 
            fig, 
            data_debugging_table)



# UNIT 7: Welche Lebensmittel werden (in einer bestimmten Mahlzeit) kombiniert
@callback(get_callback_args(unit_7, parent=unit_1))
def update_unit_7(*components):
    """
    TODO: docs
    """

    # which unit is this callback function for?
    unit = unit_7

    # initially data = None (before anything is stored into the user's browser session)
    if None in components: 
        raise PreventUpdate

    # for mutability, attr-access etc, iterability
    components = make_handy_namespace(components)
 
    # Read data fom json stored in user's browser session
    df_eating = read_json(components.json_eating)

    # Get the default values for the selectors
    default_values = get_default_values(unit)
    i = len(components) - len(default_values)  # will be used as index later
    
    # Reset the selectors if ‘store‘ is the trigger
    if ctx.triggered_id == 'store_1':
        components[i:] = default_values

    # Subset the data by dates
    df_subset_dates = subset_data_by_dates(df_eating,
                                           start_date=components.start_date,
                                           end_date=components.end_date)
    
    # Subset by the two selectors on this unit
    df_subset_dates_and_selectors = subset_data_by_selector_values(df_subset_dates,
                                                                   meals_selector=components.selector1,
                                                                   symptom_selector=components.selector2)
    # Make the plot
    fig = make_figure(unit, df_subset_dates_and_selectors,
                            n_components=components.selector3,   # slider
                            color=components.selector2,          # symptom radio-buttons
                            debugging_info=components)

    # Data for the plotly-dash DataTable (in debug mode)
    data_debugging_table = to_list_of_dicts(df_subset_dates_and_selectors)

    # update
    return (*components[i:], 
            fig, 
            data_debugging_table)



# UNIT 8: Die Lebensmittel, die wahrscheinlich die Symptome verursachen
def update_unit_8(*components):
    """
    just a dummy for a function (for visual consistency)
    this unit is not updated, therefore no callback function is needed for it.
    """



if __name__ == '__main__':

    # Prints the arguments for the decorators of each of the "units"
    # i.e. the Output/Input objects
    # This info is useful for debugging the existing callback functions
    # or when adding a new unit and a corresponding callback function for it
    # Note: `components` in the respective callback function will be a tuple containing
    # the values/properties from the `Input` objects
    if DEBUG:
        print("\n\nThe arguments for the `callback` decorator-functions, which wrap up each callback-function:\n")
        unit_names = [k for k in globals().keys() if k.startswith('unit_') and k != 'unit_0']
        for unit_name in unit_names:
            unit = globals()[unit_name]
            parent_unit = None if unit is unit_1 else unit_1
            t = get_callback_args(unit, parent=parent_unit)

            print(f"UNIT = {unit_name}:")
            for e in t:
                print(repr(e))
            print(f"default values for the selectors: {get_default_values(unit)}")
            print("-" * 50)

    # comment the next line out when deploying on a server
    app.run(debug=DEBUG)



