
"""
developer helper functions
for the BesserEsser dashboard
"""

from dash import dcc, Input, Output
from dash.dash_table import DataTable
from constants import DASH_COMPONENTS_CLASSES



def get_default_values(unit):
    """
    TODO: docs
    Determine the default values of the selectors in a given unit dynamically 
    """

    # Initialize an empty list to append item to it
    collector = []

    for e in get_dash_components_from_unit(unit):
        if e.__class__ in DASH_COMPONENTS_CLASSES and hasattr(e, 'options'):
            if hasattr(e.options, '__len__') and len(e.options) > 0: 
                collector.append(e.options[0]['value'] if type(e.options[0]) is dict else e.options[0])
            else:
                collector.append(None)
        elif type(e) is dcc.DatePickerRange:
            collector.extend([e.min_date_allowed or e.start_date, e.max_date_allowed or e.end_date])
        elif type(e) is dcc.Slider:
            collector.append(e.min)
        elif type(e) is dcc.Checklist:
            "add more functionality when needed"

    return collector



def get_dash_components_from_unit(unit):
    """
    Get dash components from a unit recursively.
    Used by the get_callback_args function (see below)
    
    Args:
        unit: dash.html.Div or array of dash components
    """

    collector = []

    def recurse(element):
        # base case
        if type(element) in DASH_COMPONENTS_CLASSES:
            collector.append(element)
            return
        elif type(element) is str \
            or not(hasattr(element, '__len__')) \
            or not(hasattr(element, 'children') and element.children):
            return
        
        # recursive case
        for e in element.children if hasattr(element, 'children') else element:
            recurse(e)
            
    recurse(unit)
    return collector
    


def get_callback_args(unit, parent):
    """
    A helper function to make the decorating of a callback function easy.

    Used to create an array of plotly-dash objects (Output, Input, Status)
    which are to be passed into the decorator which nests a callback function.
    Everything is done according to the plotly-dash design.
    Normally one would need to write out manually what is passed into each decorator of each individual
    callback function. This function generates that array with the necessary arguments 
    (i.e. Output, Input, Status objects) dynamically and avoids hard-coding.
    This function will need to be changed if some other selectors are used 
    (e.g. checkbox, which hasn't been used so far) for example. 
    Otherwise one will need to resort to hard-coding and write out the arguments 
    for each and every callback-function-decorator individually - the way it is shown in the plotly-dash documentation.
    
    Returns a tuple of Output/Input objects.
    This tuple is to be unpacked when defining the decoration, like so: *get_callback_args(...)

    Note: a "unit" is a Div object with dcc components representing an individual plot

    Arguments:
        unit: ...
        parent: unit or None
    """

    collector = [[], 
                 [Input(component_id='store_1', component_property='data'),  # df_eating
                  Input(component_id='store_2', component_property='data'),  # df_symptomreprot
                  Input(component_id='store_3', component_property='data')], # df_diary
                 [], []]
    
    for e in get_dash_components_from_unit(unit):
        if type(e) is dcc.DatePickerRange:
            collector[0].append(Output(e, component_property='start_date'))
            collector[0].append(Output(e, component_property='end_date'))
            collector[-1].append(Input(e, component_property='start_date'))
            collector[-1].append(Input(e, component_property='end_date'))
        elif type(e) is dcc.Graph:
            collector[0].append(Output(e, component_property='figure'))
        elif type(e) is DataTable:
            collector[0].append(Output(e, component_property='data'))
        elif hasattr(e, 'value'):
            collector[0].append(Output(e, component_property='value'))
            collector[-1].append(Input(e, component_property='value'))
        elif True:
            "add more hard-coded logic as necessary"
            
    if parent:
        for e in get_dash_components_from_unit(parent):
            if type(e) is dcc.DatePickerRange:
                collector[2].append(Input(e, component_property='start_date'))
                collector[2].append(Input(e, component_property='end_date'))
            elif hasattr(e, 'value'):
                collector[2].append(Input(e, component_property='value'))
            elif True:
                "add more hard-coded logic as necessary"
    
    return list(sum(collector, []))



def make_handy_namespace(components):
    """
    collections.namedtuple or typing.NamedTuple
    won't do the job - not possible to create them dynamically AND use indexing AND be mutable
    """

    class Components():
        def __init__(self, components):
            self.__dict__['list'] = list(components)
            attributes = [
                  "json_eating",   # comment
                  "json_symptomreport",  # comment
                  "json_diary",
                  "start_date", 
                  "end_date", 
                  "timespan_dropdown",  # comment
                  ]
            self.__dict__['attributes'] = attributes + [f"selector{i+1}" for i in range(len(components)-len(attributes))]
            for k,v in zip(self.attributes, self.list):
                self.__dict__[k] = v
    
        def __getitem__(self, index_or_slice):
            return self.list[index_or_slice]
        def __setitem__(self, index_or_slice, value):
            self.list[index_or_slice] = value
            [setattr(self, k, v) for k,v in zip(self.attributes, self.list)]
        def __setattr__(self, attr, value):
            self.__dict__[attr] = value
            self.list[self.attributes.index(attr)] = value
        def __len__(self):
            return len(self.list)
        def __str__(self):
            temp = [e if len(str(e))<20 else "json..." for e in self.list]
            return f"{self.__class__.__name__}({temp})"
        def __repr__(self):
            return self.__str__()
    
    ins = Components(components)
    return ins

