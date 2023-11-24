
"""
"Units" for the BesserEsser dashboard
unit = annotation + selectors + graph

Define your new unit here, and import it in app.py
"""


from dash import dcc, html
from dash.dash_table import DataTable
from PIL import Image
import os

from developer_toolkit import get_dash_components_from_unit
from table_toolkit import instantiate_debug_table
from constants import (DEBUG, BANNER_PATH, MIN_DATE, MAX_DATE, MEALS_MAPPING, ACCOUNT, 
                       TITLE_UNIT_1, TITLE_UNIT_2, TITLE_UNIT_3, TITLE_UNIT_4, TITLE_UNIT_5, 
                       TITLE_UNIT_6, TITLE_UNIT_7, TITLE_UNIT_8,
                       TEXT_UNIT_0, TEXT_UNIT_1, TEXT_UNIT_5, TEXT_UNIT_7, TEXT_FOOTER,
                       A, B, C, D, E, css)



#### Define your units here ####

# The header / banner
img = Image.open(BANNER_PATH) if os.path.exists(BANNER_PATH) else None

header = html.Div(
    style=css,  # just to show how to use css here
    children=[html.Img(src=img, height=150, width="100%")]
)


# UNIT 0: the "Konto Suchen" section
unit_0 = html.Div([
        html.Div([
            dcc.Input(placeholder=f"{ACCOUNT}nummer", id="unit_0_inputbox", type='number', debounce=True),
            html.Button(TEXT_UNIT_0, id='unit_0_button', n_clicks=0),
            html.P(id='unit_0_message_1', style={'color': 'red'})   # account not found message
                ]),
        html.H1("", id="unit_0_message_2", style={'textAlign':'center'}),  # "Informationen über..."
        dcc.Store(id="store_1"),   # df_eating (the merged table)
        dcc.Store(id="store_2"),   # df_symptomreport
        dcc.Store(id="store_3")    # df_diary
    ], id='unit_0')



# UNIT 1: Zeitraum
datepickerrange = dcc.DatePickerRange(
        display_format='DD/MM/YYYY',
        min_date_allowed=MIN_DATE,
        max_date_allowed=MAX_DATE,
        initial_visible_month=MAX_DATE,
        start_date=MIN_DATE,
        end_date=MAX_DATE,
        updatemode='bothdates',
        clearable=True,
        reopen_calendar_on_clear=True,
        id='unit_1_selector_1')

dropdown = dcc.Dropdown(options=[
    {'label':"Ausgewählter Zeitraum ->", 'value':A},
    {'label':"Gesamter Zeitraum", 'value':B}, 
    {'label':"3 Monate", 'value':C},
    {'label':"4 Wochen", 'value':D}, 
    {'label':"Eine Woche", 'value':E}
                              ], value=A, id="unit_1_selector_2")

unit_1 = html.Div([
        html.H2(TITLE_UNIT_1),
        html.P(TEXT_UNIT_1),
        datepickerrange,
        dropdown
    ], id='unit_1')


# UNIT 2: Statistics / Usage overview
unit_2 = html.Div([
        html.H2(TITLE_UNIT_2),
        html.Div(style={'width': "400px"},
                 children=[ DataTable(id="unit_2_table_1", 
                                     style_cell={'textAlign': 'left'},
                                     style_header={'display': 'none'}),
                            html.Br()]),
        html.Br()], id='unit_2')



# UNIT3: Diary (graph + table)
table = DataTable(  # the diary table
                    style_data={'whiteSpace': 'normal', 'height': 'auto'},
                    page_action='none',
                    style_table={'height': '600px', 'overflowY': 'auto'},   # height of the scroll section
                    style_cell_conditional=[
                        {'if': {'column_id': 'Datum'}, 'width': '10%'},
                        {'if': {'column_id': 'Zeit'}, 'width': '20%'},
                        {'if': {'column_id': 'Lebensmittel'}, 'width': '50%'},
                        {'if': {'column_id': 'Symptome'}, 'width': '15%'},
                    ],
                    style_cell={'textAlign': 'left', 
                                'backgroundColor': 'rgb(250, 250, 250)',
                                'fontSize': "10px"},  # font size
                    style_header={
                        'backgroundColor': 'rgb(210, 210, 210)',
                        'color': 'black',
                        'fontWeight': 'bold'
                    },
                    id="unit_3_table_0"
                    )

unit_3 = html.Div([
        html.H2(TITLE_UNIT_3),
        html.Div([dcc.Graph(id="unit_3_graph_1")]),
        #html.Br(),
        html.Div([table]),
        html.Br(), html.Br(),
        instantiate_debug_table(id="unit_3_table_1")
    ], id='unit_3')


# UNIT 4: Welche Lebensmittel sind am meisten konsumiert
# dropdown_options and radio_options defined below are applied to all applicable units as well
# for this to work the generic A,B,C,D constants are used instead of explicit values
dropdown_options = [{'label': label, 'value': value} 
                    for (label, value) in 
                    zip(["Alle Mahlzeiten"] + list(MEALS_MAPPING.values()), (A,B,C,D))]

radio_options = [
    {'label': "Alle Tage", 'value': A},
    {'label': "Tage ohne Symtome", 'value': B}, 
    {'label': "Tage mit Symtomen", 'value': C}, 
    {'label': "Tage vor dokumentierten Symptomen", 'value': D}
                ]

unit_4 = html.Div([
        html.H2(TITLE_UNIT_4),
        dcc.Dropdown(options=dropdown_options, 
                     value=A, 
                     id="unit_4_selector_1"),
        dcc.RadioItems(options=radio_options,
                       value=A, 
                       id="unit_4_selector_2"),
        dcc.Graph(id="unit_4_graph_1"),
        #TODO: diary table here
        instantiate_debug_table(id="unit_4_table_1")
    ], id='unit_4')



# UNIT 5: Welche Lebensmittel wurden unmittelbar vor der Symptomentstehung gegessen
unit_5 = html.Div([
        html.H2(TITLE_UNIT_5),
        dcc.Dropdown(options=dropdown_options[1:],  # [1:] because "alle Mahlzeiten" is excluded
                     value=B,                       #  B  because "alle Mahlzeiten" is excluded
                     id="unit_5_selector_1"),
        html.Br(), html.Br(), html.Br(),
        html.Div([html.P(TEXT_UNIT_5),
                  dcc.Slider(min=1, max=10, step=1, 
                             value=1, included=False,
                             id="unit_5_selector_2")],
                  style={'width': "30%"}),
        dcc.Graph(id="unit_5_graph_1"),
        instantiate_debug_table(id="unit_5_table_1")
    ], id='unit_5')


# UNIT 6: Wie sieht ein typisches Frühstück, Mittagessen oder Abendessen aus
unit_6 = html.Div([
        html.H2(TITLE_UNIT_6),
        dcc.Dropdown(options=dropdown_options[1:],   # [1:] because "alle Mahlzeiten" is excluded
                     value=B,                        #  B  because "alle Mahlzeiten" is excluded
                     id="unit_6_selector_1"),
        dcc.RadioItems(options=radio_options, 
                       value=A, 
                       id="unit_6_selector_2"),
        dcc.Graph(id="unit_6_graph_1"),
        instantiate_debug_table(id="unit_6_table_1")
    ], id='unit_6')



# UNIT 7: Welche Lebensmittel werden (in einer bestimmten Mahlzeit) kombiniert
unit_7 = html.Div([
        html.H2(TITLE_UNIT_7),
        dcc.Dropdown(options=dropdown_options, 
                     value=A, 
                     id="unit_7_selector_1"),
        dcc.RadioItems(options=radio_options, 
                       value=A, 
                       id="unit_7_selector_2"),
        html.Div([html.P(TEXT_UNIT_7),
                  dcc.Slider(min=2, max=5, step=1, 
                             value=2, included=False,
                             id="unit_7_selector_3")],
                  style={'width': "20%"}),
        dcc.Graph(id="unit_7_graph_1"),
        instantiate_debug_table(id="unit_7_table_1")
    ], id='unit_7')


# UNIT 8: Die Lebensmittel, die wahrscheinlich die Symptome verursachen
unit_8 = html.Div([
        html.H2(TITLE_UNIT_8),
        html.Div(style={'width': "400px"},
                 children=[DataTable(page_size=20, id="unit_8_table_1",
                                     style_cell={'textAlign': 'left'},
                                     style_header={'display': 'none'})])
    ], id='unit_8')


# Footer
footer = html.Div([  
        html.Br(), html.Hr(),
        html.Footer(TEXT_FOOTER, style={'textAlign':'center'}),
    ])


# hackish code to add some styling to dropdown menus and radio button
# to make them fit into one line
# provisional code before proper styling
for u in (unit_1, unit_2, unit_3, unit_4, unit_5, unit_6, unit_7, unit_8):
    for c in get_dash_components_from_unit(u):
        if type(c) is dcc.Dropdown:
            c.style = {'width':"250px", 'float':'left', 'display':'inline-block', 'padding-right':"20px"}
        if type(c) is dcc.RadioItems:
            c.style = {'display':'inline-block'}








